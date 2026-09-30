from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "CLOSED") & F.col("customer_id").isNotNull())
    .groupBy("status", F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.sum("balance").alias("total_balance"))
)
