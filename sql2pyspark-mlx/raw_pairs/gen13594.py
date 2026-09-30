from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").asc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("transaction_txn_type", "customer_id").orderBy(F.col("transaction_txn_time").desc_nulls_last(), "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

base = (
    df_accounts
    .filter(F.col("account_type") == "LOAN")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.upper("account_type").alias("account_type_upper"),
    )
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "opened_date", "status", "account_type_upper")
)

latest_match = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.opened_date",
        "s.status",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

filtered = (
    latest_match
    .filter(F.col("opened_date").between("2024-07-02", "2025-02-01"))
    .select(
        "id",
        "customer_id",
        "status",
        "transaction_txn_time",
        "transaction_txn_type",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "status",
        "transaction_txn_time",
        "transaction_txn_type",
        "status_title",
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy(F.col("transaction_txn_time").desc_nulls_last())
)
