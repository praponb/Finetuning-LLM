from pyspark.sql import functions as F

flattened = df_carts.select("id", "created_at", "coupon_codes", F.inline("items"))

result = (
    flattened
    .groupBy(F.when(F.col("qty") >= 5, "HIGH").otherwise("LOW").alias("qty_tier"))
    .agg(
        F.max(F.when(F.col("promo") == "WELCOME5", F.col("product_id"))).alias("welcome5_max_product_id"),
        F.percentile_approx("qty", [0.25, 0.5, 0.75]).alias("qty_quartiles"),
    )
    .filter(F.col("welcome5_max_product_id") > 0)
)
