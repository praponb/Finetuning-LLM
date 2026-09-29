from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter((F.col("page").eqNullSafe(F.lit("/search"))) & F.col("page").isin(["/product", "/search"]))
    .withColumn("event_type_length", F.length("event_type"))
    .withColumn("clean_event_type", F.trim("event_type"))
)

prev_events = cleaned.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy(
        "session_id",
        F.date_trunc("week", "event_time").alias("week_start"),
        "event_type",
    )
    .agg(
        F.percentile_approx("new_group_flag", 0.5).alias("new_group_flag_p50"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.max(F.when(F.col("event_type") == "purchase", F.col("event_type_length"))).alias("purchase_max_event_type_length"),
        F.count("*").alias("event_count"),
        F.min("prev_event_time").alias("first_prev_event_time"),
    )
)
