from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([15, 23, 40]) | F.col("product_id").isin([26, 30, 33]))
)
