from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc_nulls_first(), "id")
w2 = Window.partitionBy("merchant").orderBy(F.col("least_amount_rank_in_group").desc(), "id")
w3 = Window.partitionBy("account_id", "merchant").orderBy(F.col("amount").desc_nulls_first())
w4 = Window.partitionBy(F.year("txn_time")).orderBy("txn_time", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("merchant").orderBy("amount", "id")
w7 = Window.partitionBy(F.year("txn_time"), "merchant").orderBy("rank_in_group_2", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w9 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("merchant").orderBy(F.col("amount_dense_rank").desc_nulls_first(), "id")

deduped = (
    df_transactions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    deduped
    .filter((F.col("txn_type") == "CREDIT") & (F.col("merchant").eqNullSafe(F.lit("Starbucks"))))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "rank_in_group",
        F.least("amount", "rank_in_group").alias("least_amount_rank_in_group"),
    )
)

deduped_transactions = (
    base
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
)

metrics = (
    deduped_transactions
    .filter(F.col("least_amount_rank_in_group") > 23)
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "rank_in_group_2",
        F.dense_rank().over(w3).alias("amount_dense_rank"),
        F.avg("amount").over(w4).alias("amount_avg_5_prior"),
        F.avg("least_amount_rank_in_group").over(w5).alias("least_amount_rank_in_group_avg_90d"),
        F.lead("least_amount_rank_in_group", 1, 0).over(w6).alias("next_least_amount_rank_in_group"),
        F.avg("amount").over(w7).alias("amount_avg_3"),
    )
)

flagged = (
    metrics
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w8) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "amount_dense_rank",
        "amount_avg_5_prior",
        "amount_avg_3",
        F.sum("is_new_session").over(w9).alias("session_no"),
    )
)

deduped_transactions_2 = (
    sessionized
    .withColumn("rank_in_group", F.row_number().over(w10))
    .filter(F.col("rank_in_group") == 1)
)

filtered = (
    deduped_transactions_2
    .filter(F.col("txn_time") >= F.lit("2023-10-31 11:00:00").cast("timestamp"))
    .select(
        "*",
        F.when(F.col("amount_avg_5_prior") >= 4052, "High").when(F.col("amount_avg_5_prior") >= 1920, "Medium").otherwise("Low").alias("amount_avg_5_prior_level"),
    )
)

result = (
    filtered
    .groupBy("session_no")
    .agg(
        F.percentile_approx("amount_dense_rank", [0.25, 0.5, 0.75]).alias("amount_dense_rank_quartiles"),
        F.min("txn_time").alias("started_at"),
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
