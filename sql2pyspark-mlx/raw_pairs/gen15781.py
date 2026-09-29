from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("merchant").orderBy("last3_sum_amount", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("merchant").orderBy(F.col("last3_sum_is_new_session").desc_nulls_first(), "id")
w7 = Window.partitionBy("merchant").orderBy(F.col("last3_sum_amount").desc(), "id")
w_ordered = Window.partitionBy("merchant").orderBy("last3_sum_amount", "id")

enriched = (
    df_transactions
    .filter(F.col("amount") >= 2100)
    .select(
        "id",
        "txn_time",
        "merchant",
        F.sum("amount").over(w).alias("last3_sum_amount"),
        F.min("amount").over(w).alias("last3_min_amount"),
        F.max("amount").over(w).alias("last3_max_amount"),
        F.count("amount").over(w2).alias("amount_count_30d"),
    )
)

with_gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w3) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w4))

metrics = (
    sessions_transactions
    .select(
        "*",
        F.sum("is_new_session").over(w_grp).alias("last3_sum_is_new_session"),
        F.avg("is_new_session").over(w_grp).alias("last3_avg_is_new_session"),
        F.max("is_new_session").over(w_grp).alias("last3_max_is_new_session"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

valid = (
    latest
    .filter(F.col("merchant").isNotNull() & (F.col("last3_min_amount") > 3800))
    .withColumn("session_id_per_last3_sum_is_new_session", (F.col("session_id") / F.nullif("last3_sum_is_new_session", F.lit(0))))
    .withColumn("last3_sum_amount_whole", F.ceil("last3_sum_amount"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("OTHER")))
)

scored = (
    valid
    .select(
        "id",
        "txn_time",
        "merchant",
        "last3_sum_amount",
        "last3_min_amount",
        "amount_count_30d",
        "is_new_session",
        "session_id",
        "last3_avg_is_new_session",
        "rank_in_group",
        "session_id_per_last3_sum_is_new_session",
        "merchant_clean",
        F.lag("session_id_per_last3_sum_is_new_session").over(w_ordered).alias("prev_session_id_per_last3_sum_is_new_session"),
        F.lead("last3_sum_amount").over(w_ordered).alias("next_last3_sum_amount"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

result = (
    scored
    .groupBy("merchant")
    .agg(
        F.percentile_approx("last3_sum_amount", [0.25, 0.5, 0.75]).alias("last3_sum_amount_quartiles"),
        F.count_if(F.col("merchant") == "7-Eleven").alias("7_eleven_events"),
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_rows"),
        F.count_if(F.col("merchant") == "Starbucks").alias("starbucks_events"),
    )
)
