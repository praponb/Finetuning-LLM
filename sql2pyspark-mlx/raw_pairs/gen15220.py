from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "txn_time"), "account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy(F.year("txn_time"), "status").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

with_windows = (
    df_transactions
    .select(
        "*",
        F.sum("amount").over(w).alias("running_amount"),
        F.sum("amount").over(w2).alias("running_amount_2"),
    )
)

enriched = (
    with_windows
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "left")
    .select("src.*", F.col("a.status"), F.col("a.balance"), F.col("a.opened_date"))
)

matched_keys = (
    enriched
    .select("txn_type")
    .subtract(
        df_transactions
        .filter(F.col("amount") >= 2200)
        .select("txn_type")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

enriched_transactions = (
    kept_rows
    .select(
        "id",
        "txn_type",
        "merchant",
        "running_amount",
        "balance",
        F.count("balance").over(w_grp).alias("balance_count_90d"),
        F.sum("running_amount_2").over(w4).alias("running_amount_2_sum_90d"),
    )
)

result = (
    enriched_transactions
    .select(
        "id",
        "txn_type",
        "running_amount",
        "balance",
        "balance_count_90d",
        "running_amount_2_sum_90d",
    )
    .orderBy(F.col("running_amount").desc_nulls_first())
)
