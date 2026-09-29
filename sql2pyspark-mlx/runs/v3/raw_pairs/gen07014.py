from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.max("amount").alias("max_amount"),
        F.sum("amount").alias("total_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy(F.col("max_amount").desc())
)
