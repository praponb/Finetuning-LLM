from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee")
w2 = Window.orderBy(F.col("monthly_fee").desc_nulls_last())
w3 = Window.orderBy(F.col("monthly_fee_cume_dist").asc_nulls_last())
w4 = Window.partitionBy(F.year("end_date")).orderBy(F.col("monthly_fee").desc_nulls_last())
w5 = Window.orderBy(F.col("monthly_fee_cume_dist").desc())
w6 = Window.orderBy(F.col("monthly_fee_cume_dist").desc_nulls_first())
w7 = Window.orderBy(F.col("monthly_fee_cume_dist_percent_rank_2").desc_nulls_first())

kept_keys = (
    df_subscriptions
    .select("plan")
    .subtract(
        df_subscriptions
        .filter(F.col("plan") != "standard")
        .select("plan")
    )
)

reconciled = (
    df_subscriptions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        F.cume_dist().over(w).alias("monthly_fee_cume_dist"),
    )
)

metrics = (
    with_windows
    .withColumn("monthly_fee_percent_rank", F.percent_rank().over(w2))
    .withColumn("monthly_fee_cume_dist_percent_rank", F.percent_rank().over(w3))
    .withColumn("monthly_fee_dense_rank", F.dense_rank().over(w4))
    .withColumn("monthly_fee_cume_dist_percent_rank_2", F.percent_rank().over(w5))
    .withColumn("monthly_fee_cume_dist_dense_rank", F.dense_rank().over(w6))
)

base = (
    metrics
    .filter(F.col("monthly_fee_percent_rank") > 0.59)
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "monthly_fee_cume_dist",
        "monthly_fee_percent_rank",
        "monthly_fee_cume_dist_percent_rank",
        "monthly_fee_dense_rank",
        "monthly_fee_cume_dist_percent_rank_2",
        F.least("monthly_fee_cume_dist", "monthly_fee_dense_rank").alias("least_monthly_fee_cume_dist_monthly_fee_dense_rank"),
        F.coalesce("monthly_fee_cume_dist_percent_rank", F.lit(0)).alias("monthly_fee_cume_dist_percent_rank_filled"),
    )
)

scored = (
    base
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "monthly_fee_cume_dist",
        "monthly_fee_percent_rank",
        "monthly_fee_dense_rank",
        "least_monthly_fee_cume_dist_monthly_fee_dense_rank",
        F.dense_rank().over(w7).alias("monthly_fee_cume_dist_percent_rank_2_dense_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("least_monthly_fee_cume_dist_monthly_fee_dense_rank") >= 77, "PREMIUM").otherwise("STANDARD").alias("least_monthly_fee_cume_dist_monthly_fee_dense_rank_tier"),
    )
    .agg(
        F.sum("least_monthly_fee_cume_dist_monthly_fee_dense_rank").alias("total_least_monthly_fee_cume_dist_monthly_fee_dense_rank"),
        F.round(F.avg("monthly_fee_cume_dist"), 2).alias("avg_monthly_fee_cume_dist"),
        F.avg("monthly_fee_dense_rank").alias("avg_monthly_fee_dense_rank"),
    )
    .orderBy(F.col("avg_monthly_fee_dense_rank").desc_nulls_first())
)
