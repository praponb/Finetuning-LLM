from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("txn_type", "customer_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_accounts
    .filter(~F.col("account_type").isin(["CHECKING", "CREDIT", "SAVINGS"]))
    .select(
        "*",
        F.floor("balance").alias("balance_whole"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_filled"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time"), F.col("c.amount"))
)

with_windows = (
    latest_match
    .withColumn("last4_sum_balance", F.sum("balance").over(w2))
    .withColumn("last4_max_balance", F.max("balance").over(w2))
    .withColumn("last4_avg_balance", F.avg("balance").over(w2))
    .withColumn("balance_whole_avg_3", F.avg("balance_whole").over(w3))
)

latest_match_accounts = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.merchant").alias("transaction_merchant"),
        F.col("c.txn_type"),
    )
)

with_windows_accounts = (
    latest_match_accounts
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        "txn_time",
        "last4_sum_balance",
        "txn_type",
        F.max("last4_sum_balance").over(w5).alias("running_max_last4_sum_balance"),
        F.min("last4_sum_balance").over(w5).alias("running_min_last4_sum_balance"),
        F.avg("last4_sum_balance").over(w5).alias("running_avg_last4_sum_balance"),
    )
)

result = (
    with_windows_accounts
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        "txn_time",
        "last4_sum_balance",
        "txn_type",
        "running_max_last4_sum_balance",
        "running_min_last4_sum_balance",
        F.when(F.col("account_type").rlike("^(SAVINGS|CREDIT)$"), "EXTENDED").otherwise("GROUP_B").alias("account_type_zone"),
    )
    .orderBy(F.col("status").desc())
)
