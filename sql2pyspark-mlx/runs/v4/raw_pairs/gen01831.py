from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.quarter("paid_at").alias("paid_quarter"),
        F.when(F.col("amount") >= 1230, "high").when(F.col("amount") >= 700, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count("*").alias("row_count"))
)
