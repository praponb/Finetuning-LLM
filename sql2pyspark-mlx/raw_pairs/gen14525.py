from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attributes",
        F.array_contains(F.map_keys("attributes"), "gift").alias("has_gift"),
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
    )
)

result = (
    unpacked
    .groupBy("customer_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.sum("max_item_price").alias("total_max_item_price"),
        F.sum("max_item_price").alias("total_max_item_price_2"),
    )
    .filter(F.col("total_max_item_price_2") > 0)
    .orderBy(F.col("total_max_item_price").asc_nulls_last())
)
