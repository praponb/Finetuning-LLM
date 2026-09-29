from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy(F.col("transaction_txn_time").desc_nulls_last(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w4 = Window.orderBy(F.col("opened_date").desc(), "id")

with_last = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.opened_date",
        F.col("c.amount"),
        F.col("c.txn_time").alias("transaction_txn_time"),
    )
)

ranked = (
    with_last
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "opened_date", "transaction_txn_time")
)

with_last_accounts = (
    ranked
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.transaction_txn_time")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time_2"))
)

scored = with_last_accounts.select("*", F.row_number().over(w4).alias("row_seq"))

remaining_keys = (
    scored
    .select("id")
    .subtract(
        df_accounts
        .filter(F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]))
        .select("id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.to_date("transaction_txn_time_2") >= "2024-05-01")
    .select(
        "id",
        "opened_date",
        "transaction_txn_time",
        "transaction_txn_time_2",
        F.round((F.col("row_seq") * 0.85), 2).alias("row_seq_scaled"),
    )
)

result = (
    filtered
    .select(
        "id",
        "opened_date",
        "transaction_txn_time",
        "transaction_txn_time_2",
        "row_seq_scaled",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
