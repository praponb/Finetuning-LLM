from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("account_id")
w3 = Window.partitionBy("status", "account_id")
w4 = Window.partitionBy("account_type")
w_part = Window.orderBy(F.col("amount").desc())
w6 = Window.partitionBy("account_account_type_2")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

enriched = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.status"), F.col("p.opened_date"), F.col("p.account_type"))
)

kept_keys = (
    enriched
    .select("merchant")
    .exceptAll(
        df_transactions
        .filter(F.col("txn_type").isin(["REFUND", "CREDIT", "DEBIT"]))
        .select("merchant")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "*",
        F.count("amount").over(w).alias("amount_count_30d"),
        F.count("amount").over(w2).alias("account_id_count_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
    )
)

metrics = (
    with_windows
    .withColumn("account_type_stddev_samp_amount_share", F.stddev_samp("amount_share").over(w4))
    .withColumn("account_id_min_amount", F.min("amount").over(w2))
)

with_dims = (
    metrics
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "left")
    .select(
        "src.*",
        F.col("b.opened_date").alias("account_opened_date"),
        F.col("b.account_type").alias("account_account_type"),
        F.col("b.balance"),
    )
)

with_dims_transactions = (
    with_dims
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.account_type").alias("account_account_type_2"),
        F.col("p.opened_date").alias("account_opened_date_2"),
        F.col("p.balance").alias("account_balance"),
    )
)

with_windows_transactions = (
    with_dims_transactions
    .select(
        "*",
        F.rank().over(w_part).alias("amount_rank"),
        F.count("balance").over(w6).alias("account_account_type_2_count_balance"),
    )
)

result = (
    with_windows_transactions
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "is_flagged",
        "status",
        "opened_date",
        "account_type",
        "amount_count_30d",
        F.when(F.col("txn_type").rlike("^(CREDIT|REFUND)$"), "GROUP_A").otherwise("GROUP_B").alias("txn_type_zone"),
        F.col("sum_transaction_amount"),
    )
    .orderBy(F.col("txn_type_zone").desc_nulls_last(), "opened_date")
)
