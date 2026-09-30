from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 40) & (F.col("product_id").isin([10, 23, 26, 37]) | F.col("order_id").isin([12, 13, 24])))
)
