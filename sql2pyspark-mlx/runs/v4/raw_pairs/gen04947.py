from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "created_at",
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("none")).alias("utm_source_or_default"),
        F.col("ship_to.city").alias("ship_city"),
        F.array_contains(F.map_keys("attributes"), "utm_source").alias("has_utm_source"),
        F.size("items").alias("num_items"),
    )
)

result = (
    parsed
    .groupBy("ship_city", F.date_trunc("day", "created_at").alias("activity_day"))
    .agg(
        F.count(F.when(F.col("ship_city") == "Bangkok", 1)).alias("bangkok_count"),
        F.sort_array(F.collect_set("ship_city")).alias("ship_city_list"),
        F.round(F.avg("cart_value"), 2).alias("avg_cart_value"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.avg("cart_value").alias("avg_cart_value_2"),
        F.percentile_approx("num_items", 0.95).alias("num_items_p95"),
    )
    .orderBy("ship_city")
)
