from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "device").orderBy("started_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("started_at_hour").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

base = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-07-26").cast("date"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.hour("started_at").alias("started_at_hour"),
        F.coalesce("browser", F.lit("OTHER")).alias("browser_clean"),
    )
)

enriched = (
    base
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.city"))
)

enriched_sessions = (
    enriched
    .select(
        "*",
        F.last("started_at_hour", ignorenulls=True).over(w).alias("last_known_started_at_hour"),
        F.ntile(10).over(w2).alias("started_at_hour_bucket"),
    )
)

joined = (
    enriched_sessions
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.segment"), F.col("b.country"), F.col("b.email"))
)

latest_match = (
    joined
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page"))
)

result = (
    latest_match
    .groupBy(
        "browser",
        F.when(F.col("last_known_started_at_hour") >= 68, "PREMIUM").otherwise("STANDARD").alias("last_known_started_at_hour_tier"),
    )
    .agg(
        F.max(F.when(F.col("browser") == "Chrome", F.col("started_at_hour_bucket"))).alias("chrome_max_started_at_hour_bucket"),
        F.round(F.avg("last_known_started_at_hour"), 2).alias("avg_last_known_started_at_hour"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.percentile_approx("started_at_hour_bucket", 0.5).alias("started_at_hour_bucket_p50"),
        F.min("started_at").alias("started_at"),
    )
    .filter(F.col("avg_last_known_started_at_hour") < 0)
    .orderBy(F.col("consumer_count").asc_nulls_last())
)
