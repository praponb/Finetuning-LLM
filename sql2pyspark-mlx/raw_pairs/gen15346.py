from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status", "account_id").orderBy("txn_time", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("account_type", "account_id").orderBy("txn_type_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        F.when(F.col("amount") >= 3100, "High").when(F.col("amount") >= 1700, "Medium").otherwise("Low").alias("amount_level"),
        F.length("txn_type").alias("txn_type_length"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_filled"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.account_type"), F.col("b.status"))
)

scored = (
    joined
    .select(
        "id",
        "account_id",
        "txn_type",
        "account_type",
        "status",
        F.avg("txn_type_length").over(w_recent).alias("txn_type_length_avg_5_prior"),
        F.first("account_type", ignorenulls=True).over(w2).alias("first_known_account_type"),
    )
)

remaining_keys = (
    scored
    .select("id")
    .intersectAll(
        df_transactions
        .filter(F.col("txn_type") != "REFUND")
        .select("id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "inner")
    .select(
        "e.account_id",
        "e.status",
        "e.txn_type_length_avg_5_prior",
        F.col("p.status").alias("account_status"),
    )
)

enriched_transactions = (
    enriched
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .filter(F.col("b.balance") >= 29000)
    .select(
        "e.account_id",
        "e.txn_type_length_avg_5_prior",
        F.col("b.status").alias("account_status_2"),
        F.col("b.balance"),
        F.col("b.opened_date"),
    )
)

joined_transactions = (
    enriched_transactions
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "inner")
    .select("e.account_id", "e.account_status_2", F.col("p.status"))
)

summary = (
    joined_transactions
    .rollup("status")
    .agg(
        F.count(F.when(F.col("account_status_2") == "OPEN", 1)).alias("open_count"),
        F.sort_array(F.collect_set("account_status_2")).alias("account_status_2_list"),
        F.count_if(F.col("account_status_2") == "FROZEN").alias("frozen_events"),
    )
)

result = (
    summary
    .select("status", "open_count", "account_status_2_list", "frozen_events")
    .orderBy(F.col("status").desc_nulls_last())
)
