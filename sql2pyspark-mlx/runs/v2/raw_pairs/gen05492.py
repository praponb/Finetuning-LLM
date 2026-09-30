from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(22000, 76000) & F.col("account_type").isin(["SAVINGS", "CREDIT", "CHECKING"]))
    .groupBy("account_type")
    .agg(F.max("opened_date").alias("latest_opened_date"))
    .orderBy("latest_opened_date", "account_type")
)
