from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["CREDIT", "CHECKING", "SAVINGS"]))
    .groupBy("status", "account_type")
    .agg(F.min("opened_date").alias("min_opened_date"))
    .orderBy("status", "account_type")
    .limit(20)
)
