from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("ship_city").orderBy(F.col("distinct_ids").desc())

flattened_attrs = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "ship_to",
        F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x["qty"] * x["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        "ship_city",
        F.when(F.col("cart_value") >= 1570, "PREMIUM").otherwise("STANDARD").alias("cart_value_tier"),
    )
    .agg(
        F.max(F.when(F.col("ship_city") == "Singapore", F.col("cart_value"))).alias("singapore_max_cart_value"),
        F.sort_array(F.collect_set("ship_city")).alias("ship_city_list"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .withColumn("distinct_ids_rank", F.rank().over(w))
)
