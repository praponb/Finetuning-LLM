from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.month("txn_time"), "account_id").orderBy(F.col("txn_time").desc(), "id")
w3 = Window.partitionBy("txn_type")
w_part = Window.partitionBy("merchant").orderBy("amount_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("txn_type").orderBy("running_min_amount_share", "id")

ranked = (
    df_transactions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    ranked
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "is_flagged",
        F.row_number().over(w2).alias("row_seq"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
    )
)

metrics_transactions = (
    metrics
    .filter(F.col("amount_share") <= 0.3)
    .select(
        "id",
        "txn_type",
        "merchant",
        "is_flagged",
        "row_seq",
        "amount_share",
        F.sum("amount_share").over(w_part).alias("running_sum_amount_share"),
        F.min("amount_share").over(w_part).alias("running_min_amount_share"),
        F.max("amount_share").over(w_part).alias("running_max_amount_share"),
    )
)

latest = (
    metrics_transactions
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

stats = (
    latest
    .withColumn("row_seq_tier", F.when(F.col("row_seq") >= 6, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["txn_type", "row_seq_tier"], ["txn_type"]],
        "txn_type",
        "row_seq_tier",
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_rows"),
        F.round(F.avg("running_min_amount_share"), 2).alias("avg_running_min_amount_share"),
        F.percentile_approx("running_min_amount_share", [0.25, 0.5, 0.75]).alias("running_min_amount_share_quartiles"),
        F.grouping("txn_type").alias("grouping_id"),
    )
)

result = (
    stats
    .select(
        "txn_type",
        "row_seq_tier",
        "fee_rows",
        "avg_running_min_amount_share",
        "running_min_amount_share_quartiles",
        "grouping_id",
    )
    .orderBy(F.col("grouping_id").desc())
)
