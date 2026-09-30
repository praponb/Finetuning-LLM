from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") < 37000) & (F.col("opened_date") > "2024-11-13"))
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.count("*").alias("account_count"),
        F.sum("balance").alias("total_balance"),
        F.count("balance").alias("balance_count"),
    )
)
