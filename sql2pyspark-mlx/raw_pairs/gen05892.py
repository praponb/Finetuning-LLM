from pyspark.sql import functions as F

result = (
    df_accounts
    .rollup(F.quarter("opened_date").alias("opened_quarter"), "customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
    )
)
