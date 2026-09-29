from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.size("attributes").alias("num_attributes"),
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.size(F.filter("items", lambda x: x["qty"] >= 3)).alias("bulk_lines"),
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.col("attributes")["utm_source"].alias("utm_source"),
    )
)

result = (
    unpacked
    .withColumn("bulk_lines_tier", F.when(F.col("bulk_lines") >= 3, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["customer_id", "bulk_lines_tier"], ["customer_id"], []],
        "customer_id",
        "bulk_lines_tier",
    )
    .agg(
        F.countDistinct("utm_source").alias("distinct_utm_sources"),
        F.countDistinct("utm_source").alias("distinct_utm_sources_2"),
        F.grouping_id().alias("grouping_level"),
    )
)
