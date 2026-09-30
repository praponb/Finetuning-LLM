from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") >= 0.31) | F.col("amount").between(900, 1500))
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(F.count("shipping_fee").alias("shipping_fee_count"))
    .orderBy(F.col("order_quarter").desc())
)
