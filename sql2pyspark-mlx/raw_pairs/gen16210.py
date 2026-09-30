from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "ship_to",
        F.coalesce(F.col("attributes").getItem("device"), F.lit("direct")).alias("device_or_default"),
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
        F.size("items").alias("num_items"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("num_items") >= 3, "HIGH").otherwise("LOW").alias("num_items_tier"),
    )
    .agg(
        F.sum("num_items").alias("total_num_items"),
        F.percentile_approx("num_items", 0.5).alias("num_items_p50"),
        F.max("num_items").alias("max_num_items"),
    )
)
