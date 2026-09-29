from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.year("paid_at").alias("paid_year"), "method")
    .agg(
        F.count("amount").alias("amount_count"),
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("num_payments"),
    )
    .filter(F.col("total_amount") > 2600)
    .orderBy(F.col("method").desc())
)
