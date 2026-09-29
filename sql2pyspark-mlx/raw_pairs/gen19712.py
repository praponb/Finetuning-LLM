from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("is_flagged_flag").desc(), "id")
w_running = Window.partitionBy("account_id").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-04-17 07:00:00").cast("timestamp")) & (F.col("merchant").eqNullSafe(F.lit("Shell"))))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

joined = (
    base
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "left")
    .filter(F.col("p.customer_id").isin([3, 15, 38]))
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        "src.amount",
        "src.txn_time",
        "src.merchant",
        "src.is_flagged_flag",
        "src.is_flagged_label",
        F.col("p.status"),
        F.col("p.opened_date"),
    )
)

deduped = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.sum("is_flagged_flag").over(w_running).alias("running_sum_is_flagged_flag"),
        F.count("is_flagged_flag").over(w_running).alias("running_count_is_flagged_flag"),
        F.max("is_flagged_flag").over(w_running).alias("running_max_is_flagged_flag"),
        F.avg("is_flagged_flag").over(w_running).alias("running_avg_is_flagged_flag"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("running_count_is_flagged_flag") >= 16, "HIGH").otherwise("LOW").alias("running_count_is_flagged_flag_tier"),
    )
    .agg(
        F.min("txn_time").alias("started_at"),
        F.percentile_approx("running_max_is_flagged_flag", 0.95).alias("running_max_is_flagged_flag_p95"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
        F.min("txn_time").alias("started_at_2"),
        F.max("txn_time").alias("last_seen"),
    )
)
