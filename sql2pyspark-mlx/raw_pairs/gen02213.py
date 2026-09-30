from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter((F.col("customer_id") == 30) & ~F.col("account_type").isin(["CHECKING", "CREDIT"]))
)

status_summary = (
    valid_accounts
    .groupBy("status")
    .agg(F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"))
)

result = (
    status_summary
    .orderBy(F.col("savings_count").desc(), "status")
    .limit(50)
)
