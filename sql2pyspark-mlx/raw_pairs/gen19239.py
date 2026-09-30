from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "key").orderBy("created_at", "id", "key")
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("created_at", "id", "key")
w4 = Window.partitionBy("customer_id", "key")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "v"),
    )
)

cleaned = (
    flattened
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .withColumn("created_at_year", F.year("created_at"))
    .withColumn("sorted_coupon_codes", F.sort_array("coupon_codes"))
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    ranked
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "key",
        F.sum("created_at_year").over(w2).alias("created_at_year_sum_15m"),
        F.row_number().over(w3).alias("event_seq"),
        F.max("created_at_year").over(w4).alias("customer_id_max_created_at_year"),
    )
)

filtered = (
    with_windows
    .filter(F.col("key").isNotNull() & ~F.col("key").isin(["weight_kg", "utm_source"]))
    .select(
        "id",
        "coupon_codes",
        "key",
        "created_at_year_sum_15m",
        "customer_id_max_created_at_year",
        F.col("event_seq").cast("double").alias("event_seq_num"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_filled"),
    )
)

filtered_carts = (
    filtered
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "key",
        "created_at_year_sum_15m",
        "customer_id_max_created_at_year",
        "key_filled",
        F.substring("key", 1, 4).alias("key_prefix"),
    )
)

result = (
    filtered_carts
    .groupBy(
        F.when(F.col("customer_id_max_created_at_year") >= 87, "LARGE").otherwise("SMALL").alias("customer_id_max_created_at_year_tier"),
        "key",
    )
    .agg(
        F.percentile_approx("created_at_year_sum_15m", 0.95).alias("created_at_year_sum_15m_p95"),
        F.round(F.sum("created_at_year_sum_15m") / F.nullif(F.count(F.when(F.col("key") == "weight_kg", 1)), F.lit(0)), 2).alias("created_at_year_sum_15m_per_weight_kg"),
        F.count(F.when(F.col("key") == "utm_source", 1)).alias("utm_source_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("key") == "utm_source", F.col("created_at_year_sum_15m"))).alias("utm_source_total_created_at_year_sum_15m"),
    )
    .filter(F.col("distinct_ids") > 1)
)
