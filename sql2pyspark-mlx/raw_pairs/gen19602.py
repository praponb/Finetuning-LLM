from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc_nulls_first(), "id")
w_part = Window.partitionBy("account_id").orderBy("amount_filled", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("status", "txn_type").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.min("amount").alias("min_transaction_amount"))

valid = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-10-24"))) & (~F.col("merchant").eqNullSafe(F.lit("Lazada"))))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("N/A")))
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

joined = (
    deduped
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        "src.is_flagged",
        "src.amount_filled",
        "src.merchant_clean",
        "src.rn",
        F.col("p.opened_date"),
        F.col("p.account_type"),
        F.col("p.status"),
    )
)

with_windows = (
    joined
    .filter(F.col("status").isin(["CLOSED", "OPEN"]))
    .select(
        "*",
        F.avg("amount_filled").over(w_part).alias("last4_avg_amount_filled"),
        F.max("amount_filled").over(w_part).alias("last4_max_amount_filled"),
        F.sum("amount_filled").over(w_part).alias("last4_sum_amount_filled"),
        F.avg("amount_filled").over(w3).alias("amount_filled_avg_3"),
    )
)

daily = (
    with_windows
    .withColumn("last4_avg_amount_filled_tier", F.when(F.col("last4_avg_amount_filled") >= 74, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["last4_avg_amount_filled_tier", "account_type", "account_id"], ["last4_avg_amount_filled_tier", "account_type"], ["last4_avg_amount_filled_tier"], []],
        "last4_avg_amount_filled_tier",
        "account_type",
        "account_id",
    )
    .agg(
        F.percentile_approx("amount_filled_avg_3", 0.9).alias("amount_filled_avg_3_p90"),
        F.round(F.avg("last4_avg_amount_filled"), 2).alias("avg_last4_avg_amount_filled"),
        F.percentile_approx("last4_avg_amount_filled", 0.95).alias("last4_avg_amount_filled_p95"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    daily
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "account_type",
        "account_id",
        "amount_filled_avg_3_p90",
        "avg_last4_avg_amount_filled",
        "last4_avg_amount_filled_p95",
        "refund_count",
        F.col("min_transaction_amount"),
    )
    .orderBy("account_type")
)
