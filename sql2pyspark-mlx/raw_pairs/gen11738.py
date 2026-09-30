from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
        F.col("ship_to.country").alias("ship_country"),
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        "ship_country",
        F.when(F.col("items_total") >= 1250, "PREMIUM").otherwise("STANDARD").alias("items_total_tier"),
    )
    .agg(
        F.percentile_approx("items_total", 0.5).alias("items_total_p50"),
        F.avg(F.when(F.col("ship_country") == "US", F.col("items_total"))).alias("us_avg_items_total"),
    )
    .filter(F.col("items_total_p50") > 0)
)
