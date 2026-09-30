from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "SAVINGS", "CHECKING"]) & ~F.col("account_type").isin(["LOAN", "CHECKING"]))
)

agg = (
    recent_accounts
    .groupBy("status")
    .agg(F.min("opened_date").alias("first_opened_date"))
)

result = (
    agg
    .select("status", "first_opened_date")
    .orderBy(F.col("first_opened_date").desc(), F.col("status").desc())
)
