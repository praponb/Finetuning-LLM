from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

cleaned = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-06-20").cast("date"))
    .withColumn("account_type_upper", F.upper("account_type"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.min("balance").over(w).alias("running_min_balance"),
        F.max("balance").over(w).alias("running_max_balance"),
        F.avg("balance").over(w).alias("running_avg_balance"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.balance",
        "s.opened_date",
        "s.account_type_upper",
        "s.running_max_balance",
        "s.running_avg_balance",
        F.col("c.txn_type").alias("transaction_txn_type"),
        F.col("c.merchant"),
    )
)

kept = (
    with_latest
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.account_id").isin([5, 18, 29, 35])), "left_semi")
)

result = (
    kept
    .groupBy("transaction_txn_type")
    .agg(
        F.sum(F.when(F.col("transaction_txn_type") == "REFUND", F.col("balance")).otherwise(0)).alias("refund_balance"),
        F.countDistinct("merchant").alias("distinct_merchants"),
        F.sum(F.when(F.col("transaction_txn_type") == "CREDIT", F.col("running_avg_balance")).otherwise(0)).alias("credit_running_avg_balance"),
    )
    .orderBy("distinct_merchants")
)
