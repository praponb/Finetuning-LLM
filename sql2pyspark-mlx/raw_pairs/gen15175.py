from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

cleaned = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .withColumn("txn_type_title", F.initcap(F.trim("txn_type")))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("N/A")))
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.account_id",
        "e.txn_type",
        "e.txn_time",
        "e.merchant",
        "e.is_flagged",
        "e.txn_type_title",
        "e.is_flagged_label",
        "e.merchant_filled",
        F.col("b.balance"),
        F.col("b.opened_date"),
    )
)

remaining_keys = (
    with_dims
    .select("account_id", "id")
    .intersectAll(
        df_transactions
        .filter(F.col("merchant").isNotNull())
        .select("account_id", "id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = verified.select("*", F.count("balance").over(w).alias("balance_count_30d"))

result = (
    enriched
    .groupBy("merchant")
    .agg(
        F.min("balance_count_30d").alias("min_balance_count_30d"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.min("txn_time").alias("first_txn_time"),
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("balance")).otherwise(0)).alias("amazon_balance"),
        F.percentile_approx("balance", 0.5).alias("balance_p50"),
    )
    .orderBy("amazon_balance")
)
