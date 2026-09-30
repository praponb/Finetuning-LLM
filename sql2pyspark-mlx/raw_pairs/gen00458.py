from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "device").orderBy("started_at", "id")
w_part = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.lower("browser").alias("browser_lower"),
        F.upper("device").alias("device_upper"),
        F.coalesce("country", F.lit("NONE")).alias("resolved_country"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

filtered = (
    ranked
    .filter(F.col("customer_id").isNotNull() & (F.col("started_at") >= F.lit("2023-02-18 17:00:00").cast("timestamp")))
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "device_upper",
        "resolved_country",
        F.initcap(F.trim("browser")).alias("browser_title"),
    )
)

enriched = (
    filtered
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "country",
        "device_upper",
        "browser_title",
        F.last("country", ignorenulls=True).over(w_part).alias("last_known_country"),
        F.last("country", ignorenulls=True).over(w_part).alias("last_known_country_2"),
    )
)

result = (
    enriched
    .groupBy("customer_id")
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.count("*").alias("event_count"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
    )
)
