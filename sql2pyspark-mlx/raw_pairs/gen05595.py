from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy(F.col("amount").desc_nulls_last(), "id")

deduped = (
    df_transactions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

remaining_keys = (
    deduped
    .select("id", "account_id")
    .subtract(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("id", "account_id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("account_id").isin([2, 24, 32, 36]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "rn",
        F.substring("merchant", 1, 2).alias("merchant_prefix"),
    )
)

with_dims = (
    base
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.account_id",
        "a.amount",
        "a.merchant",
        F.col("d.balance"),
        F.col("d.opened_date"),
    )
)

daily = (
    with_dims
    .groupBy(
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
    )
    .agg(
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("balance"))).alias("starbucks_total_balance"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "Shell", 1)), F.lit(0)), 2).alias("amount_per_shell"),
        F.countDistinct("merchant").alias("distinct_merchants"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
)

result = (
    daily
    .select(
        "year_start",
        "quarter_start",
        "starbucks_total_balance",
        "num_merchants",
        "distinct_merchants",
        "avg_balance",
    )
)
