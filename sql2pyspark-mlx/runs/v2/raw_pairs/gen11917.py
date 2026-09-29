from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([7, 8, 9]) & (F.col("status") != "RETURNED"))
    .agg(F.max("amount").alias("max_amount"))
    .orderBy(F.col("max_amount").desc())
)
