from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.to_date("txn_time")).orderBy(F.col("amount_whole").desc(), "id")
w3 = Window.partitionBy(F.month("txn_time"), "account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id", "txn_type")
w5 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w6 = Window.partitionBy("txn_type")
w7 = Window.partitionBy("txn_type").orderBy("amount_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("txn_type").orderBy("is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("txn_type").orderBy(F.col("is_new_burst").asc_nulls_last())
w11 = Window.partitionBy("is_new_burst_tier").orderBy(F.col("distinct_ids").desc())

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-04-24 11:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.floor("amount").alias("amount_whole"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

scored = (
    cleaned
    .filter(F.col("txn_type") != "FEE")
    .select(
        "*",
        F.first("merchant", ignorenulls=True).over(w).alias("first_known_merchant"),
        F.ntile(3).over(w2).alias("amount_whole_bucket"),
        F.last("amount", ignorenulls=True).over(w3).alias("last_known_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
    )
)

cleaned_transactions = (
    scored
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "amount_whole",
        "resolved_merchant",
        "last_known_amount",
        "amount_share",
        F.round((F.col("amount_share") * 1.1), 2).alias("amount_share_adjusted"),
        F.concat_ws("-", "merchant", "txn_type").alias("merchant_txn_type_key"),
    )
)

with_gaps = (
    cleaned_transactions
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w5) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w))

with_gaps_2 = (
    sessions_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w5).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w5) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_transactions_2 = with_gaps_2.withColumn("burst_id", F.sum("is_new_session").over(w))

gaps = (
    sessions_transactions_2
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w5).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w5) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id_2", F.sum("new_group_flag").over(w))

enriched = (
    sessionized
    .select(
        "id",
        "amount",
        "amount_share_adjusted",
        "is_new_burst",
        F.avg("amount_share_adjusted").over(w6).alias("txn_type_avg_amount_share_adjusted"),
        F.first("amount_whole", ignorenulls=True).over(w7).alias("first_known_amount_whole"),
        F.sum("amount_share_adjusted").over(w8).alias("running_amount_share_adjusted"),
        F.last("is_new_session", ignorenulls=True).over(w9).alias("last_known_is_new_session"),
        F.percent_rank().over(w_grp).alias("is_new_burst_percent_rank"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "LARGE").otherwise("SMALL").alias("is_new_burst_tier"),
        F.when(F.col("amount") >= 3100, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        F.when(F.col("last_known_is_new_session") >= 1, "HIGH").otherwise("LOW").alias("last_known_is_new_session_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("is_new_burst_percent_rank", [0.25, 0.5, 0.75]).alias("is_new_burst_percent_rank_quartiles"),
        F.percentile_approx("amount_share_adjusted", [0.25, 0.5, 0.75]).alias("amount_share_adjusted_quartiles"),
    )
    .filter(F.col("distinct_ids") > 1)
    .withColumn("distinct_ids_percent_rank", F.percent_rank().over(w11))
    .orderBy(F.col("amount_tier").desc_nulls_first())
)
