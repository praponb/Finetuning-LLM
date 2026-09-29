from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

filtered = (
    flattened
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-02"))) & F.col("promo").isNotNull() & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.trim("promo").alias("clean_promo"),
        F.coalesce("promo", F.lit("NONE")).alias("promo_clean"),
    )
)

cleaned = (
    filtered
    .filter(F.col("customer_id").isNotNull())
    .withColumn("product_id_band", F.when(F.col("product_id") >= 8, "High").when(F.col("product_id") >= 3, "Medium").otherwise("Low"))
)

result = (
    cleaned
    .groupBy("product_id")
    .agg(
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_count"),
        F.min("created_at").alias("started_at"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
    )
)
