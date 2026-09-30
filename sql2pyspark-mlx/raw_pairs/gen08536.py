from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc(), "id")
w2 = Window.partitionBy(F.month("txn_time"), "merchant").orderBy("amount")

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("7-Eleven"))))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.trim("merchant").alias("clean_merchant"),
        F.lower("merchant").alias("merchant_lower"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled"),
    )
)

scored = (
    base
    .select(
        "id",
        "txn_type",
        "is_flagged",
        F.row_number().over(w).alias("event_seq"),
        F.percent_rank().over(w2).alias("amount_percent_rank"),
    )
)

filtered = (
    scored
    .filter(F.col("amount_percent_rank").between(0.53, 0.6))
    .withColumn("least_event_seq_amount_percent_rank", F.least("event_seq", "amount_percent_rank"))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

cleaned = (
    filtered
    .filter(F.col("amount_percent_rank") <= 0.4)
    .select(
        "id",
        "txn_type",
        "is_flagged",
        "event_seq",
        "amount_percent_rank",
        "least_event_seq_amount_percent_rank",
        "is_flagged_label",
        (F.col("least_event_seq_amount_percent_rank") - F.col("amount_percent_rank")).alias("least_event_seq_amount_percent_rank_minus_amount_percent_rank"),
    )
)

result = (
    cleaned
    .groupBy("txn_type")
    .agg(
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
        F.percentile_approx("least_event_seq_amount_percent_rank_minus_amount_percent_rank", [0.25, 0.5, 0.75]).alias("least_event_seq_amount_percent_rank_minus_amount_percent_rank_quartiles"),
        F.round(F.sum("least_event_seq_amount_percent_rank_minus_amount_percent_rank") / F.nullif(F.count(F.when(F.col("txn_type") == "DEBIT", 1)), F.lit(0)), 2).alias("least_event_seq_amount_percent_rank_minus_amount_percent_rank_per_debit"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount_percent_rank"))).alias("debit_total_amount_percent_rank"),
    )
    .filter(F.col("debit_count") >= 1)
)
