from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("balance").between(24000, 67000))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.trim("account_type").alias("clean_account_type"),
        F.date_add("opened_date", 3).alias("opened_date_plus_3d"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("activity_month"), "status")
    .agg(
        F.count_if(F.col("account_type") == "CREDIT").alias("credit_events"),
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
        F.countDistinct("status").alias("distinct_status"),
    )
)
