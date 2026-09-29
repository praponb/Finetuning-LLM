from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(1150, 1240) | (F.col("discount") >= 0.14))
    .groupBy("customer_id")
    .agg(F.count("*").alias("total_count"))
    .orderBy(F.col("customer_id").desc())
)
