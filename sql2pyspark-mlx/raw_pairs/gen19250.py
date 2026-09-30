from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("amount_range") >= 47)
    .select("method", "amount_range", "total_amount")
    .orderBy(F.col("amount_range").desc())
)
