from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter(F.col("status").isNull() & (F.col("customer_id").isNull() | (F.col("account_type") == "LOAN")))
)

summary = (
    filtered_accounts
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "account_type")
    .agg(
        F.max("balance").alias("max_balance"),
        F.count("*").alias("row_count"),
    )
)

result = (
    summary
    .filter(F.col("row_count") < 8)
    .select("opened_quarter", "account_type", "max_balance", "row_count")
    .orderBy("row_count")
)
