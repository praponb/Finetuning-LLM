from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("txn_type").orderBy("amount_int", "id")
w5 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.orderBy("amount_avg_2_prior_adjusted", "id").rowsBetween(-5, -1)
w7 = Window.orderBy("running_amount_int", "id").rowsBetween(-5, -1)
w8 = Window.orderBy("amount_int_bucket_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("amount_avg_2_prior_adjusted", "id")

cleaned = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-11-04").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("amount").cast("int").alias("amount_int"),
        F.trim("merchant").alias("clean_merchant"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "amount",
        "txn_time",
        "is_flagged",
        "amount_int",
        "clean_merchant",
        F.dense_rank().over(w_recent).alias("amount_dense_rank"),
        F.avg("amount").over(w2).alias("amount_avg_2_prior"),
        F.sum("amount_int").over(w3).alias("running_amount_int"),
        F.ntile(5).over(w4).alias("amount_int_bucket"),
    )
)

valid = (
    enriched
    .filter(F.col("amount_dense_rank") <= 3)
    .withColumn("amount_dense_rank_adjusted", F.round((F.col("amount_dense_rank") * 0.85), 2))
    .withColumn("amount_avg_2_prior_adjusted", F.round((F.col("amount_avg_2_prior") * 1.1), 2))
)

valid_transactions = (
    valid
    .filter((F.col("txn_time") > F.lit("2023-09-23").cast("date")) & (F.to_date("txn_time") >= "2024-08-29"))
    .withColumn("amount_dense_rank_adjusted_plus_running_amount_int", (F.col("amount_dense_rank_adjusted") + F.col("running_amount_int")))
)

metrics = (
    valid_transactions
    .withColumn("amount_avg_2_prior_max_90d", F.max("amount_avg_2_prior").over(w5))
    .withColumn("amount_int_bucket_avg_5_prior", F.avg("amount_int_bucket").over(w6))
)

metrics_transactions = (
    metrics
    .filter(F.col("amount_dense_rank_adjusted") <= 76)
    .select(
        "*",
        F.avg("amount_avg_2_prior_adjusted").over(w5).alias("amount_avg_2_prior_adjusted_avg_90d"),
        F.avg("amount_dense_rank_adjusted").over(w7).alias("amount_dense_rank_adjusted_avg_5_prior"),
        F.sum("amount_dense_rank_adjusted").over(w8).alias("running_amount_dense_rank_adjusted"),
        F.first("amount", ignorenulls=True).over(w9).alias("first_known_amount"),
        F.row_number().over(w10).alias("row_seq"),
    )
)

base = (
    metrics_transactions
    .filter(F.col("txn_time") > F.lit("2023-08-29 02:00:00").cast("timestamp"))
    .withColumn("amount_dense_rank_per_amount_avg_2_prior_max_90d", F.round((F.col("amount_dense_rank") / F.col("amount_avg_2_prior_max_90d")), 2))
)

daily = (
    base
    .groupBy(
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
        F.when(F.col("amount_dense_rank_per_amount_avg_2_prior_max_90d") >= 70, "PREMIUM").otherwise("STANDARD").alias("amount_dense_rank_per_amount_avg_2_prior_max_90d_tier"),
        F.when(F.col("amount_int") >= 86, "PREMIUM").otherwise("STANDARD").alias("amount_int_tier"),
    )
    .agg(
        F.min("amount_avg_2_prior_max_90d").alias("min_amount_avg_2_prior_max_90d"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("amount_int_bucket_avg_5_prior", [0.25, 0.5, 0.75]).alias("amount_int_bucket_avg_5_prior_quartiles"),
    )
)

result = (
    daily
    .select(
        "year_start",
        "amount_dense_rank_per_amount_avg_2_prior_max_90d_tier",
        "amount_int_tier",
        "amount_int_bucket_avg_5_prior_quartiles",
    )
    .orderBy(F.col("amount_dense_rank_per_amount_avg_2_prior_max_90d_tier").desc())
)
