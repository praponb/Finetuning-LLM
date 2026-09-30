from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.to_date("event_time") < "2023-11-01")
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.lower("page").alias("page_lower"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_clean"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "page_lower",
        F.sum("is_new_session").over(w2).alias("session_id_2"),
    )
)

result = (
    sessions_web_events
    .groupBy(
        F.when(F.col("session_id_2") >= 7, "PREMIUM").otherwise("STANDARD").alias("session_id_2_tier"),
    )
    .agg(
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/product", 1)), F.lit(0)), 2).alias("duration_sec_per_product"),
        F.sum(F.when(F.col("page") == "/product", F.col("session_id_2"))).alias("product_total_session_id_2"),
        F.percentile_approx("duration_sec", 0.5).alias("duration_sec_p50"),
    )
    .filter(F.col("duration_sec_p50") >= 0)
    .orderBy(F.col("duration_sec_p50").asc_nulls_last())
)
