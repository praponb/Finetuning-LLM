from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w3 = Window.partitionBy("account_id", "merchant")

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-11-01 12:00:00").cast("timestamp")) & F.col("merchant").isNotNull() & F.col("txn_type").isin(["FEE", "DEBIT"]))
    .select(
        "*",
        F.concat("merchant", F.lit("/"), "txn_type").alias("merchant_txn_type"),
        F.lower("txn_type").alias("txn_type_lower"),
    )
)

with_windows = (
    base
    .withColumn("txn_type_count_amount", F.count("amount").over(w))
    .withColumn("next_amount", F.lead("amount", 1).over(w2))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
)

valid = (
    with_windows
    .filter(F.col("txn_time") >= F.lit("2024-02-08 16:00:00").cast("timestamp"))
    .withColumn("amount_minus_txn_type_count_amount", (F.col("amount") - F.col("txn_type_count_amount")))
    .withColumn("amount_whole", F.ceil("amount"))
)

kept_keys = (
    valid
    .select("account_id")
    .subtract(
        df_transactions
        .filter(F.col("account_id").isin([4, 27, 36, 38]))
        .select("account_id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "merchant_txn_type",
        "txn_type_lower",
        "txn_type_count_amount",
    )
    .orderBy(F.col("txn_type").desc())
)
