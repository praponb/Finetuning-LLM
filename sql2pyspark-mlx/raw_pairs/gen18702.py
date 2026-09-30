from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "txn_type").orderBy("txn_time_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id")
w3 = Window.partitionBy("account_status").orderBy("txn_time", "id")
w4 = Window.partitionBy("account_status").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-04-20").cast("date")) & (F.col("merchant").eqNullSafe(F.lit("Amazon"))))
    .select(
        "*",
        F.to_date("txn_time").alias("txn_time_date"),
        F.col("txn_time").cast("date").alias("txn_time_day"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("resolved_merchant"),
    )
)

enriched = (
    filtered
    .filter(F.col("txn_time_date") > "2025-02-01")
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
)

with_dims = (
    enriched
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.amount",
        "s.txn_time",
        "s.merchant",
        "s.resolved_merchant",
        "s.amount_share",
        F.col("b.balance"),
        F.col("b.opened_date"),
        F.col("b.status"),
    )
)

enriched_transactions = (
    with_dims
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "inner")
    .select(
        "a.*",
        F.col("b.opened_date").alias("account_opened_date"),
        F.col("b.account_type"),
        F.col("b.status").alias("account_status"),
    )
)

flagged = (
    enriched_transactions
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w3).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w3) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_burst").over(w4))

kept_keys = (
    islands
    .select("id", "merchant")
    .intersect(
        df_transactions
        .filter(F.col("txn_type") == "FEE")
        .select("id", "merchant")
    )
)

kept_rows = (
    islands
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "inner")
    .rollup(
        F.col("p.account_type").alias("account_account_type"),
        F.col("a.status").alias("transaction_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("a.balance")).alias("max_balance"),
        F.sum(F.col("a.amount")).alias("total_amount"),
    )
    .orderBy(F.col("total_amount").desc())
)
