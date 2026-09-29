from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-03-31 05:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.length("page").alias("page_length"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

flagged = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("duration_sec") >= 362, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
        F.date_trunc("quarter", "event_time").alias("quarter_start"),
        "event_type",
    )
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.percentile_approx("duration_sec", 0.9).alias("duration_sec_p90"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("event_type") == "add_to_cart", 1)), F.lit(0)), 2).alias("session_start_flag_per_add_to_cart"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
    )
)
