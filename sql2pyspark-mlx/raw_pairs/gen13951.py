from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2025-10-01")
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.min("balance").alias("min_balance"))
    .orderBy(F.col("min_balance").desc())
)
