from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") != "CREDIT") & (F.col("account_type") == "SAVINGS") & (F.col("opened_date") <= "2023-05-22"))
    .groupBy(F.year("opened_date").alias("opened_year"), "status")
    .agg(F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"))
)
