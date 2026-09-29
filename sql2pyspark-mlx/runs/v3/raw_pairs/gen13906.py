from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["SAVINGS", "CREDIT", "LOAN"]) | (F.year("opened_date") == 2024))
    .groupBy("account_type")
    .agg(F.countDistinct("balance").alias("unique_customers"))
    .orderBy("account_type")
)
