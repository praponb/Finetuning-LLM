from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") < 6)
    .select("order_id", F.ceil("quantity").alias("quantity_whole"))
)
