from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy("balance", "id")
w2 = Window.partitionBy("txn_type").orderBy("next_balance", "id")
w3 = Window.orderBy("prev_balance", "id")
w4 = Window.partitionBy("merchant")
w5 = Window.orderBy("merchant_min_prev_balance", "id")

with_dims = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "left")
    .filter(F.year(F.col("d.opened_date")) >= 2025)
    .select(
        "e.id",
        "e.account_id",
        "e.txn_type",
        "e.txn_time",
        "e.merchant",
        "e.is_flagged",
        F.col("d.balance"),
    )
)

enriched = (
    with_dims
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "is_flagged",
        F.lead("balance").over(w).alias("next_balance"),
        F.lag("balance").over(w).alias("prev_balance"),
        F.row_number().over(w).alias("seq_no"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "merchant", "is_flagged", "prev_balance")
)

with_windows = (
    ranked
    .select(
        "id",
        "is_flagged",
        F.lag("prev_balance", 2).over(w3).alias("prev_prev_balance"),
        F.min("prev_balance").over(w4).alias("merchant_min_prev_balance"),
    )
)

enriched_transactions = (
    with_windows
    .withColumn("prev_prev_prev_balance", F.lag("prev_prev_balance").over(w5))
    .withColumn("next_prev_prev_balance", F.lead("prev_prev_balance").over(w5))
    .withColumn("next_merchant_min_prev_balance", F.lead("merchant_min_prev_balance").over(w5))
    .withColumn("row_seq", F.row_number().over(w5))
)

result = (
    enriched_transactions
    .groupBy(
        F.when(F.col("row_seq") >= 7, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
        F.when(F.col("next_merchant_min_prev_balance") >= 62575, "PREMIUM").otherwise("STANDARD").alias("next_merchant_min_prev_balance_tier"),
    )
    .agg(
        F.avg("row_seq").alias("avg_row_seq"),
        F.percentile_approx("merchant_min_prev_balance", 0.9).alias("merchant_min_prev_balance_p90"),
        F.count("*").alias("event_count"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.round(F.avg("prev_prev_prev_balance"), 2).alias("avg_prev_prev_prev_balance"),
    )
)
