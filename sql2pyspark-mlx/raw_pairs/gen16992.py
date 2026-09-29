from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") > "2023-10-12") & (F.col("status") == "FROZEN") & F.col("customer_id").isNotNull())
    .groupBy("customer_id")
    .agg(F.count("*").alias("account_count"))
)
