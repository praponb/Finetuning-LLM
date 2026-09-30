from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~((F.col("amount") <= 630) & F.col("order_id").isin([29, 33])))
    .agg(
        F.max("amount").alias("max_amount"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy(F.col("max_amount").desc())
)
