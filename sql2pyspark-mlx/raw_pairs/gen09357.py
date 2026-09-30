from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("transaction_txn_type").orderBy("is_checking", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

eligible = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date")), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

filtered = (
    latest_match
    .filter(F.to_date("transaction_txn_time") >= "2023-10-31")
    .select(
        "id",
        "balance",
        "status",
        "transaction_txn_time",
        "transaction_txn_type",
        F.when(F.col("account_type") == "CHECKING", 1).otherwise(0).alias("is_checking"),
        F.coalesce("status", F.lit("N/A")).alias("status_clean"),
    )
)

latest_match_accounts = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant"), F.col("c.amount").alias("transaction_amount"))
)

enriched = (
    latest_match_accounts
    .withColumn("running_avg_transaction_amount", F.avg("transaction_amount").over(w2))
    .withColumn("running_min_transaction_amount", F.min("transaction_amount").over(w2))
    .withColumn("running_count_transaction_amount", F.count("transaction_amount").over(w2))
    .withColumn("running_max_transaction_amount", F.max("transaction_amount").over(w2))
)

result = (
    enriched
    .select(
        "id",
        "balance",
        "status",
        "status_clean",
        "transaction_amount",
        "running_avg_transaction_amount",
        "running_min_transaction_amount",
        "running_count_transaction_amount",
        "running_max_transaction_amount",
    )
    .orderBy(F.col("running_min_transaction_amount").desc_nulls_last())
)
