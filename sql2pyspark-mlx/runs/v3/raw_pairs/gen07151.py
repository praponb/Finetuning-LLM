from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") <= 7) & (F.col("product_id").isNotNull() | F.col("product_id").isNull()))
    .withColumn("unit_price_whole", F.ceil("unit_price"))
    .orderBy("order_id")
)
