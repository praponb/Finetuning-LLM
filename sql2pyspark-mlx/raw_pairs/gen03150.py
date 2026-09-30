from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "attributes",
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.size("items").alias("num_items"),
    )
)

result = (
    unpacked
    .groupBy(
        "customer_id",
        F.when(F.col("weight_kg") >= 3.6, "PREMIUM").otherwise("STANDARD").alias("weight_kg_tier"),
    )
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.round(F.avg("weight_kg"), 2).alias("avg_weight_kg"),
        F.percentile_approx("weight_kg", 0.95).alias("weight_kg_p95"),
    )
)
