from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

joined = (
    cleaned
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "left")
    .select("e.*", F.col("a.status"), F.col("a.opened_date"))
)

base = (
    joined
    .filter(F.col("merchant").isNotNull())
    .withColumn("clean_txn_type", F.trim("txn_type"))
)

joined_transactions = (
    base
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "inner")
    .select(
        "e.*",
        F.col("d.account_type"),
        F.col("d.opened_date").alias("account_opened_date"),
        F.col("d.balance"),
    )
)

result = (
    joined_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "is_flagged_flag",
        "is_flagged_label",
        "opened_date",
        F.when(F.col("txn_type").rlike("^(REFUND|DEBIT)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("txn_type_zone"),
    )
    .orderBy(F.col("opened_date").desc_nulls_first())
)
