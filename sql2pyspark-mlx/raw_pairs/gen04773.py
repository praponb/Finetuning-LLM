from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("is_search", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("duration_sec")
w3 = Window.partitionBy("country").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device", "browser").orderBy("is_search", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("device", "browser").orderBy(F.col("is_search").asc_nulls_first(), "id")

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("page").eqNullSafe(F.lit("/home"))))
    .withColumn("duration_sec_str", F.col("duration_sec").cast("string"))
    .withColumn("is_search", F.when(F.col("page") == "/search", 1).otherwise(0))
    .withColumn("resolved_page", F.coalesce("page", F.lit("UNKNOWN")))
)

enriched = (
    base
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.country").isin(["US", "UK"]))
    .select(
        "src.id",
        "src.session_id",
        "src.event_type",
        "src.event_time",
        "src.duration_sec",
        "src.duration_sec_str",
        "src.is_search",
        F.col("s.device"),
        F.col("s.country"),
        F.col("s.browser"),
    )
)

scored = (
    enriched
    .withColumn("duration_sec_avg_5", F.avg("duration_sec").over(w))
    .withColumn("duration_sec_rank", F.rank().over(w2))
    .withColumn("first_known_device", F.first("device", ignorenulls=True).over(w3))
    .withColumn("is_search_avg_2_prior", F.avg("is_search").over(w4))
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .rollup(
        F.when(F.col("duration_sec_avg_5") >= 521, "HIGH").otherwise("LOW").alias("duration_sec_avg_5_tier"),
    )
    .agg(
        F.percentile_approx("is_search", [0.25, 0.5, 0.75]).alias("is_search_quartiles"),
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("duration_sec_p95").asc_nulls_last())
)
