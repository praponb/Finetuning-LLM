from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("ship_country").orderBy("max_item_price", "id")
w2 = Window.orderBy(F.col("max_item_price").desc(), "id")

unpacked = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "ship_to",
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.col("ship_to.country").alias("ship_country"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("direct")).alias("utm_source_or_default"),
    )
)

result = (
    unpacked
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("row_seq_2", F.row_number().over(w2))
)
