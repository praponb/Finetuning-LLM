from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        "attributes",
        F.col("attributes")["device"].alias("device"),
        F.col("ship_to.city").alias("ship_city"),
        F.transform(
            F.filter("items", lambda i: i["promo"].isNotNull()),
            lambda i: F.struct(i["product_id"].alias("pid"), F.round(i["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
    )
)

result = (
    parsed
    .groupBy("ship_city")
    .agg(
        F.sort_array(F.collect_set("ship_city")).alias("ship_city_list"),
        F.count(F.when(F.col("ship_city") == "New York", 1)).alias("new_york_count"),
        F.countDistinct("device").alias("distinct_devices"),
    )
    .orderBy(F.col("distinct_devices").desc_nulls_first())
)
