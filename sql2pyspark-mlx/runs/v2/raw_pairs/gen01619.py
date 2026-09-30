from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") < "2025-01-06") & (F.col("account_type") == "LOAN"))
    .agg(F.countDistinct("balance").alias("unique_statuss"))
    .orderBy("unique_statuss")
)
