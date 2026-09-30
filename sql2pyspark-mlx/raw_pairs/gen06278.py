from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("merchant", "account_id")
w3 = Window.partitionBy("txn_type")
w4 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy(F.month("txn_time"), "account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("txn_type", "merchant").orderBy("amount_avg_7d", "id").rowsBetween(-2, -1)
w8 = Window.partitionBy("txn_type").orderBy("running_sum_amount_share_2", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy("txn_type").orderBy(F.col("last4_min_amount_share").desc_nulls_last(), "id")
w10 = Window.partitionBy("merchant").orderBy("running_min_amount_share_2", "id")

scored = (
    df_transactions
    .filter(F.col("merchant") == "Lazada")
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.sum("amount").over(w).alias("amount_sum_90d"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share_2"),
        F.sum("amount").over(w4).alias("running_amount"),
        F.avg("amount").over(w_grp).alias("amount_avg_7d"),
    )
)

cleaned = (
    scored
    .filter(F.col("is_flagged"))
    .withColumn("running_amount_num", F.col("running_amount").cast("string"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("OTHER")))
)

with_windows = (
    cleaned
    .withColumn("running_max_amount_share_2", F.max("amount_share_2").over(w6))
    .withColumn("running_sum_amount_share_2", F.sum("amount_share_2").over(w6))
    .withColumn("running_min_amount_share_2", F.min("amount_share_2").over(w6))
    .withColumn("amount_avg_2_prior", F.avg("amount").over(w7))
)

scored_transactions = (
    with_windows
    .select(
        "*",
        F.sum("amount_share").over(w8).alias("last4_sum_amount_share"),
        F.count("amount_share").over(w8).alias("last4_count_amount_share"),
        F.max("amount_share").over(w8).alias("last4_max_amount_share"),
        F.min("amount_share").over(w8).alias("last4_min_amount_share"),
    )
)

latest = (
    scored_transactions
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
)

ranked = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w10))
    .filter(F.col("rank_in_group") == 1)
)

totals = (
    ranked
    .groupBy("merchant")
    .agg(
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.percentile_approx("running_amount", 0.9).alias("running_amount_p90"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.avg(F.when(F.col("merchant") == "Starbucks", F.col("amount_avg_7d"))).alias("starbucks_avg_amount_avg_7d"),
    )
)

result = (
    totals
    .select("merchant", "num_merchants", "txn_type_list")
    .orderBy(F.col("num_merchants").desc())
)
