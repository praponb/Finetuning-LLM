from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.col("created_at") < "2023-11-01")
    .select(
        "id",
        "created_at",
        "items",
        "ship_to",
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.col("attributes")["gift"].alias("gift"),
        F.size("items").alias("num_items"),
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.percentile_approx("max_item_price", [0.25, 0.5, 0.75]).alias("max_item_price_quartiles"),
        F.min("max_item_price").alias("min_max_item_price"),
    )
    .orderBy("activity_day")
)
