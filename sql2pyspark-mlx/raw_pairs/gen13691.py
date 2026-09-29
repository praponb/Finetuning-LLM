from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("amount", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("Lazada"))))
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.round("amount", 2).alias("amount_rounded"),
        F.month("txn_time").alias("txn_time_month"),
    )
)

with_windows = (
    filtered
    .filter(F.col("merchant") != "Shell")
    .select(
        "id",
        "amount",
        "amount_rounded",
        F.avg("amount").over(w).alias("running_avg_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
        F.count("amount").over(w).alias("running_count_amount"),
    )
)

valid = (
    with_windows
    .filter(F.col("running_count_amount") <= 8)
    .select(
        "id",
        "amount",
        "amount_rounded",
        "running_avg_amount",
        "running_max_amount",
        "running_count_amount",
        F.ceil("running_count_amount").alias("running_count_amount_whole"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.min("running_max_amount").over(w2).alias("last4_min_running_max_amount"),
        F.max("running_max_amount").over(w2).alias("last4_max_running_max_amount"),
        F.count("running_max_amount").over(w2).alias("last4_count_running_max_amount"),
    )
)

filtered_transactions = (
    scored
    .filter(F.col("amount").between(1400, 3100))
    .select(
        "*",
        F.when(F.col("amount_rounded") >= 66, "High").when(F.col("amount_rounded") >= 30, "Medium").otherwise("Low").alias("amount_rounded_tier"),
    )
)

daily = (
    filtered_transactions
    .withColumn("running_max_amount_tier", F.when(F.col("running_max_amount") >= 3800, "PREMIUM").otherwise("STANDARD"))
    .withColumn("last4_count_running_max_amount_tier", F.when(F.col("last4_count_running_max_amount") >= 15, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["running_max_amount_tier", "last4_count_running_max_amount_tier"], ["running_max_amount_tier"], []],
        "running_max_amount_tier",
        "last4_count_running_max_amount_tier",
    )
    .agg(
        F.percentile_approx("running_count_amount", 0.95).alias("running_count_amount_p95"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    daily
    .select(
        "last4_count_running_max_amount_tier",
        "running_count_amount_p95",
        "distinct_ids",
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
