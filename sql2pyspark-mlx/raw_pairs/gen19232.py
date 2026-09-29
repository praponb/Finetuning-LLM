from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy(F.col("island_id_plus_amount").asc_nulls_last(), "id")
w4 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w5 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("txn_type").orderBy("island_id_plus_amount_filled", "id").rowsBetween(-3, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

base = (
    islands
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-04-12"))) & F.col("merchant").isNotNull())
    .withColumn("amount_rounded", F.round("amount", 0))
    .withColumn("island_id_plus_amount", (F.col("island_id") + F.col("amount")))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("OTHER")))
)

cleaned = (
    base
    .filter(F.col("txn_time") >= F.to_timestamp(F.lit("2023-06-12 05:00:00")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "island_id",
        "amount_rounded",
        "island_id_plus_amount",
        "resolved_merchant",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0).alias("is_credit"),
    )
)

cleaned_transactions = (
    cleaned
    .filter(F.col("txn_time") > F.lit("2023-07-02 20:00:00").cast("timestamp"))
    .select(
        "*",
        F.coalesce("island_id_plus_amount", F.lit(0)).alias("island_id_plus_amount_filled"),
    )
)

latest = (
    cleaned_transactions
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

with_gaps = (
    latest
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w4).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w4) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "is_flagged",
        "is_flagged_flag",
        "island_id_plus_amount_filled",
        "row_num",
        F.sum("session_start_flag").over(w5).alias("island_id_2"),
    )
)

metrics = (
    sessionized
    .select(
        "id",
        "txn_time",
        "is_flagged",
        "is_flagged_flag",
        "island_id_plus_amount_filled",
        "row_num",
        F.sum("island_id_plus_amount_filled").over(w_hist).alias("last4_sum_island_id_plus_amount_filled"),
        F.count("island_id_plus_amount_filled").over(w_hist).alias("last4_count_island_id_plus_amount_filled"),
        F.avg("island_id_plus_amount_filled").over(w_hist).alias("last4_avg_island_id_plus_amount_filled"),
        F.min("island_id_plus_amount_filled").over(w_hist).alias("last4_min_island_id_plus_amount_filled"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("last4_min_island_id_plus_amount_filled") >= 63, "LARGE").otherwise("SMALL").alias("last4_min_island_id_plus_amount_filled_tier"),
        F.date_trunc("MONTH", "txn_time").alias("activity_month"),
    )
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.percentile_approx("last4_min_island_id_plus_amount_filled", [0.25, 0.5, 0.75]).alias("last4_min_island_id_plus_amount_filled_quartiles"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("is_flagged_flag", [0.25, 0.5, 0.75]).alias("is_flagged_flag_quartiles"),
        F.round(F.avg("last4_count_island_id_plus_amount_filled"), 2).alias("avg_last4_count_island_id_plus_amount_filled"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
