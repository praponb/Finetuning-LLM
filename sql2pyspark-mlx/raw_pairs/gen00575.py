from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("event_time") >= "2023-05-01")
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.length("event_type").alias("event_type_length"),
        F.ceil("duration_sec").alias("duration_sec_whole"),
    )
)

flagged = (
    filtered
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec_whole",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    islands
    .groupBy("burst_id")
    .agg(
        F.percentile_approx("burst_id", 0.5).alias("burst_id_p50"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_rows"),
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
    )
    .filter(F.col("checkout_rows") > 1)
)
