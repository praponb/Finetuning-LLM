from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("start_date").desc())
w2 = Window.partitionBy("status").orderBy("end_date", "id")
w_hist = Window.orderBy(F.col("monthly_fee_whole").asc_nulls_last())
w4 = Window.orderBy(F.col("monthly_fee_whole").desc_nulls_last())
w5 = Window.orderBy(F.col("monthly_fee_whole").desc())
w_ordered = Window.orderBy(F.col("monthly_fee_whole_cume_dist").desc_nulls_first())
w_recent = Window.orderBy(F.col("monthly_fee_whole_rank").desc_nulls_first())
w8 = Window.orderBy(F.col("monthly_fee_whole_dense_rank").desc_nulls_last())

cleaned = (
    df_subscriptions
    .filter(F.col("start_date") >= F.to_date(F.lit("2023-03-13")))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "status",
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
    )
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest_subscriptions = (
    latest
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
    .select("id", "monthly_fee_whole", "rank_in_group")
)

kept_keys = (
    latest_subscriptions
    .select("id")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date") <= "2024-11-30")
        .select("id")
    )
)

verified = (
    latest_subscriptions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("monthly_fee_whole_dense_rank", F.dense_rank().over(w_hist))
    .withColumn("monthly_fee_whole_rank", F.rank().over(w4))
    .withColumn("monthly_fee_whole_cume_dist", F.cume_dist().over(w5))
)

metrics = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w_ordered).alias("monthly_fee_whole_cume_dist_cume_dist"),
    )
)

metrics_subscriptions = (
    metrics
    .filter(F.col("monthly_fee_whole_dense_rank") == 5)
    .select(
        "id",
        "monthly_fee_whole",
        "monthly_fee_whole_dense_rank",
        F.dense_rank().over(w_recent).alias("monthly_fee_whole_rank_dense_rank"),
        F.dense_rank().over(w8).alias("monthly_fee_whole_dense_rank_dense_rank"),
    )
)

result = (
    metrics_subscriptions
    .withColumn("monthly_fee_whole_dense_rank_tier", F.when(F.col("monthly_fee_whole_dense_rank") >= 7, "LARGE").otherwise("SMALL"))
    .withColumn("monthly_fee_whole_rank_dense_rank_tier", F.when(F.col("monthly_fee_whole_rank_dense_rank") >= 8, "PREMIUM").otherwise("STANDARD"))
    .withColumn("monthly_fee_whole_dense_rank_dense_rank_tier", F.when(F.col("monthly_fee_whole_dense_rank_dense_rank") >= 7, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["monthly_fee_whole_dense_rank_tier", "monthly_fee_whole_rank_dense_rank_tier", "monthly_fee_whole_dense_rank_dense_rank_tier"], ["monthly_fee_whole_dense_rank_tier", "monthly_fee_whole_rank_dense_rank_tier"], ["monthly_fee_whole_dense_rank_tier"]],
        "monthly_fee_whole_dense_rank_tier",
        "monthly_fee_whole_rank_dense_rank_tier",
        "monthly_fee_whole_dense_rank_dense_rank_tier",
    )
    .agg(
        F.percentile_approx("monthly_fee_whole", [0.25, 0.5, 0.75]).alias("monthly_fee_whole_quartiles"),
        F.count("*").alias("row_count"),
        F.round(F.avg("monthly_fee_whole_rank_dense_rank"), 2).alias("avg_monthly_fee_whole_rank_dense_rank"),
        F.percentile_approx("monthly_fee_whole_dense_rank_dense_rank", [0.25, 0.5, 0.75]).alias("monthly_fee_whole_dense_rank_dense_rank_quartiles"),
    )
    .orderBy(F.col("monthly_fee_whole_rank_dense_rank_tier").desc())
)
