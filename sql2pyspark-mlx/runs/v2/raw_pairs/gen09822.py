from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([20, 24, 34]) & F.col("quantity").isNotNull())
    .select("unit_price", F.ceil("unit_price").alias("unit_price_whole"))
)
