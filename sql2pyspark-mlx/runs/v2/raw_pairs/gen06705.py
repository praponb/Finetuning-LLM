from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", "method")
    .agg(
        F.max("amount").alias("max_amount"),
        F.count("*").alias("num_payments"),
    )
    .orderBy(F.col("num_payments").desc())
)
