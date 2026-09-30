from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").asc_nulls_first(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("balance").asc_nulls_last(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("transaction_merchant").orderBy(F.col("amount").desc_nulls_first(), "id")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-08-16"))) & F.col("customer_id").isNotNull())
    .select("*", F.col("balance").cast("int").alias("balance_int"))
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "customer_id", "balance", "opened_date", "balance_int")
)

deduped = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

latest_match = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.balance",
        "s.opened_date",
        "s.balance_int",
        F.col("c.amount"),
        F.col("c.merchant").alias("transaction_merchant"),
    )
)

deduped_accounts = (
    latest_match
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "balance", "amount", "transaction_merchant")
)

with_latest = (
    deduped_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.amount",
        "s.transaction_merchant",
        F.col("c.amount").alias("transaction_amount"),
        F.col("c.txn_type"),
    )
)

result = (
    with_latest
    .select("id", "amount", "transaction_merchant", "transaction_amount", "txn_type")
    .orderBy("txn_type")
)
