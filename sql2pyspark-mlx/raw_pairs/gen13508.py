from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("qty")

exploded = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
    )
)

scored = exploded.withColumn("qty_percent_rank", F.percent_rank().over(w))

cleaned = (
    scored
    .filter(F.col("qty_percent_rank") > 0.21)
    .select(
        "id",
        "price",
        "qty_percent_rank",
        F.when(F.col("product_id") >= 10, "High").when(F.col("product_id") >= 3, "Medium").otherwise("Low").alias("product_id_level"),
    )
)

cleaned_carts = (
    cleaned
    .filter(F.col("qty_percent_rank") > 0.61)
    .withColumn("price_level", F.when(F.col("price") >= 240, "High").when(F.col("price") >= 63, "Medium").otherwise("Low"))
)

base = (
    cleaned_carts
    .filter(F.col("qty_percent_rank") >= 0.76)
    .select("*", F.col("price").cast("int").alias("price_int"))
)

result = base.select("id", "qty_percent_rank", "product_id_level", "price_level")
