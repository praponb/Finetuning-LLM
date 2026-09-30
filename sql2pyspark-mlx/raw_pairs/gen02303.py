from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_status_2", "status").orderBy(F.col("opened_date").desc_nulls_last(), "id")

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

joined = (
    filtered
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .filter(F.col("d.account_type") == "LOAN")
    .select(
        "a.id",
        "a.account_id",
        "a.amount",
        "a.txn_time",
        "a.is_flagged_label",
        F.col("d.opened_date"),
        F.col("d.status"),
        F.col("d.balance"),
    )
)

enriched = (
    joined
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "inner")
    .select(
        "s.id",
        "s.account_id",
        "s.is_flagged_label",
        "s.opened_date",
        "s.status",
        "s.balance",
        F.col("a.account_type"),
        F.col("a.status").alias("account_status"),
        F.col("a.opened_date").alias("account_opened_date"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.balance").alias("account_balance"),
        F.col("b.status").alias("account_status_2"),
        F.col("b.opened_date").alias("account_opened_date_2"),
    )
)

deduped = (
    with_dims
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy("account_status_2")
    .pivot("account_status", ["OPEN", "CLOSED"])
    .agg(F.max("account_balance").alias("amount"))
    .select(
        "account_status_2",
        F.col("OPEN").alias("open"),
        F.col("CLOSED").alias("closed"),
    )
)
