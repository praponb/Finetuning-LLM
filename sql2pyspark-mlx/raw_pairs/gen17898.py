from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("account_status", "txn_type").orderBy(F.col("amount").desc_nulls_first(), "id")

valid = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.concat("merchant", F.lit("/"), "txn_type").alias("merchant_txn_type"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "inner")
    .select("s.*", F.col("a.balance"), F.col("a.status"))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "inner")
    .filter(F.year(F.col("d.opened_date")) == 2024)
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.amount",
        "a.balance",
        F.col("d.account_type"),
        F.col("d.opened_date"),
        F.col("d.status").alias("account_status"),
    )
)

latest = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "txn_type", "amount", "balance", "account_status")
)

latest_transactions = (
    latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest_transactions
    .select(
        "id",
        "amount",
        "balance",
        "account_status",
        F.when(F.col("account_status").rlike("^(OPEN|FROZEN)$"), "GROUP_B").when(F.col("account_status").rlike("^(CLOSED)$"), "GROUP_A").otherwise("PRIORITY").alias("account_status_zone"),
    )
    .orderBy("balance", "account_status")
)
