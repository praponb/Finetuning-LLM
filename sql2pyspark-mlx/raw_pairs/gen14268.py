from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_running = Window.partitionBy("promo")
w3 = Window.partitionBy(F.year("created_at"), "promo")

lines = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

base = (
    lines
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "product_id",
        "qty",
        "promo",
        (F.col("price") * F.col("qty")).alias("price_times_qty"),
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
        F.coalesce("promo", F.lit("OTHER")).alias("resolved_promo"),
    )
)

valid = (
    base
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 11:00:00"))) & F.col("promo").isNotNull())
    .select("*", F.upper("promo").alias("promo_upper"))
)

base_carts = (
    valid
    .filter(F.col("created_at") > F.lit("2025-03-01 22:00:00").cast("timestamp"))
    .select(
        "*",
        (F.col("qty") / F.nullif("price_times_qty", F.lit(0))).alias("qty_per_price_times_qty"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("promo_filled"),
    )
)

with_windows = (
    base_carts
    .withColumn("qty_count_2h", F.count("qty").over(w))
    .withColumn("promo_min_qty_per_price_times_qty", F.min("qty_per_price_times_qty").over(w_running))
    .withColumn("created_at_year_stddev_samp_product_id", F.stddev_samp("product_id").over(w3))
    .withColumn("promo_min_qty_per_price_times_qty_2", F.min("qty_per_price_times_qty").over(w_running))
)

filtered = (
    with_windows
    .filter((F.col("created_at") >= F.lit("2025-03-01 18:00:00").cast("timestamp")) & (F.col("promo").eqNullSafe(F.lit("SPRING10"))))
    .select(
        "created_at",
        "coupon_codes",
        "product_id",
        "promo",
        "price_times_qty",
        "resolved_promo",
        "promo_upper",
        "qty_per_price_times_qty",
        "created_at_year_stddev_samp_product_id",
        "promo_min_qty_per_price_times_qty_2",
        F.col("qty_count_2h").cast("string").alias("qty_count_2h_str"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("promo_filled_2"),
    )
)

result = (
    filtered
    .groupBy("product_id")
    .agg(
        F.round(F.avg("price_times_qty"), 2).alias("avg_price_times_qty"),
        F.min("created_at").alias("started_at"),
        F.min("product_id").alias("min_product_id"),
        F.sum(F.when(F.col("promo") == "WELCOME5", F.col("product_id"))).alias("welcome5_total_product_id"),
    )
)
