from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("transaction_txn_type").orderBy("transaction_txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("transaction_txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("transaction_txn_type").orderBy("transaction_txn_time", "id")

base = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-12-15").cast("date"))
    .select(
        "id",
        "customer_id",
        "status",
        F.when(F.col("balance") >= 61000, "High").when(F.col("balance") >= 14000, "Medium").otherwise("Low").alias("balance_band"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type").alias("transaction_txn_type"))
)

filtered = (
    latest_match
    .filter(F.col("transaction_txn_type").isin(["CREDIT", "FEE"]))
    .withColumn("transaction_txn_type_filled", F.coalesce("transaction_txn_type", F.lit("unassigned")))
    .withColumn("status_filled", F.coalesce("status", F.lit("DIRECT")))
)

filtered_accounts = (
    filtered
    .filter(F.col("customer_id").isin([19, 21, 33]))
    .select(
        "id",
        "customer_id",
        "status",
        "balance_band",
        "transaction_txn_type",
        "status_filled",
        F.lower("status").alias("status_lower"),
        F.coalesce("transaction_txn_type", F.lit("Unknown")).alias("transaction_txn_type_filled_2"),
    )
)

with_latest = (
    filtered_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time"))
)

metrics = (
    with_latest
    .withColumn("last_known_transaction_txn_type", F.last("transaction_txn_type", ignorenulls=True).over(w3))
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w4))
)

with_gaps = (
    metrics
    .withColumn("session_start_flag", F.when(F.lag("transaction_txn_time").over(w5).isNull() | (F.col("transaction_txn_time") > F.lag("transaction_txn_time").over(w5) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w3))

result = (
    sessionized
    .select(
        "id",
        "balance_band",
        "status_filled",
        "status_lower",
        "transaction_txn_time",
        "last_known_transaction_txn_type",
        "first_known_status",
        "session_start_flag",
        "session_id",
    )
)
