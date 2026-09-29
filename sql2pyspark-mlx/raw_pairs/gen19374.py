from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy(F.month("txn_time")).orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant", "account_id").orderBy("amount", "id")
w4 = Window.partitionBy("merchant").orderBy("cumulative_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_transactions
    .select(
        "*",
        F.count("amount").over(w).alias("amount_count_7d"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "amount_count_7d",
        F.lead("cumulative_amount").over(w3).alias("next_cumulative_amount"),
        F.lag("cumulative_amount").over(w3).alias("prev_cumulative_amount"),
        F.lead("amount_count_7d").over(w3).alias("next_amount_count_7d"),
        F.row_number().over(w3).alias("event_seq"),
        F.sum("amount_count_7d").over(w4).alias("running_amount_count_7d"),
    )
)

remaining_keys = (
    scored
    .select("account_id", "id")
    .subtract(
        df_transactions
        .filter(F.col("merchant") != "Grab")
        .select("account_id", "id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.account_type"), F.col("p.balance"))
)

result = (
    enriched
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .rollup(
        F.col("p.account_type").alias("account_account_type"),
        F.col("p.status").alias("status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("e.amount_count_7d")).alias("avg_amount_count_7d"),
    )
    .orderBy(F.col("avg_amount_count_7d").desc_nulls_last())
)
