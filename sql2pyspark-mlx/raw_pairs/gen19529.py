from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("merchant").orderBy("transaction_txn_time", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "OPEN", "FROZEN"]) & (~F.col("status").eqNullSafe(F.lit("CLOSED"))))
    .select("*", F.trim("account_type").alias("clean_account_type"))
)

base = (
    filtered
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-08-18"))) & (F.year("opened_date") == 2025))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "clean_account_type",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
        F.date_trunc("month", "opened_date").alias("opened_date_period"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.opened_date",
        "s.status",
        "s.balance_filled",
        F.col("c.merchant"),
        F.col("c.txn_time").alias("transaction_txn_time"),
    )
)

with_windows = (
    with_latest
    .select(
        "id",
        "account_type",
        "status",
        "balance_filled",
        "transaction_txn_time",
        F.sum("balance_filled").over(w_part).alias("last4_sum_balance_filled"),
        F.min("balance_filled").over(w_part).alias("last4_min_balance_filled"),
        F.avg("balance_filled").over(w_part).alias("last4_avg_balance_filled"),
    )
)

result = (
    with_windows
    .groupBy(
        "account_type",
        F.date_trunc("QUARTER", "transaction_txn_time").alias("quarter_start"),
        F.when(F.col("last4_min_balance_filled") >= 78, "LARGE").otherwise("SMALL").alias("last4_min_balance_filled_tier"),
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("last4_avg_balance_filled")).otherwise(0)).alias("credit_last4_avg_balance_filled"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_rows"),
        F.percentile_approx("balance_filled", [0.25, 0.5, 0.75]).alias("balance_filled_quartiles"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.min("transaction_txn_time").alias("first_transaction_txn_time"),
    )
)
