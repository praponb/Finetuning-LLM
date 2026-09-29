from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([7, 21, 33, 36]))
    .withColumn("quantity_num", F.col("quantity").cast("double"))
)
