from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", "status", "customer_id")
    .agg(F.countDistinct("balance").alias("unique_statuss"))
    .orderBy(F.col("account_type").desc())
)
