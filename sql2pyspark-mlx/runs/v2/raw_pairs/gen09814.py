from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "LOAN", "CHECKING"]) & (F.col("opened_date") > "2023-09-12"))
    .groupBy("status")
    .agg(F.max("opened_date").alias("latest_opened_date"))
)
