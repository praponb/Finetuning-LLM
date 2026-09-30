from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") > 1510) | F.col("order_id").isin([2, 8]))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy("max_amount")
)
