from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "OPEN") & F.month("opened_date").isin([5, 6, 7]) & F.col("account_type").isin(["CREDIT", "LOAN", "SAVINGS"]))
    .groupBy("account_type")
    .agg(F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"))
    .filter(F.col("closed_count") >= 2)
    .orderBy("account_type")
)
