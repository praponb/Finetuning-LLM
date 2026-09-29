from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "created_at"), "customer_id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("code").orderBy(F.col("weeks_since_created_at").desc_nulls_last())
w_ordered = Window.partitionBy("customer_id").orderBy(F.col("created_at_period_max_weeks_since_created_at").desc_nulls_first())
w_recent = Window.partitionBy("customer_id")
w_recent_2 = Window.partitionBy("customer_id").orderBy("created_at_period_max_weeks_since_created_at")

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("code"))
)

cleaned = (
    exploded
    .filter(F.col("customer_id").isin([11, 25, 36, 39]) & F.col("customer_id").isNotNull())
    .withColumn("weeks_since_created_at", (F.datediff(F.current_date(), "created_at") / 7))
    .withColumn("created_at_label", F.date_format("created_at", "yyyy-MM"))
    .withColumn("resolved_code", F.coalesce("code", F.lit("N/A")))
)

with_windows = (
    cleaned
    .withColumn("created_at_period_max_weeks_since_created_at", F.max("weeks_since_created_at").over(w))
)

with_windows_carts = (
    with_windows
    .select(
        "id",
        "customer_id",
        "created_at_label",
        "created_at_period_max_weeks_since_created_at",
        F.count("weeks_since_created_at").over(w2).alias("weeks_since_created_at_count_15m"),
        F.cume_dist().over(w3).alias("weeks_since_created_at_cume_dist"),
    )
)

scored = (
    with_windows_carts
    .select(
        "created_at_label",
        "weeks_since_created_at_cume_dist",
        F.cume_dist().over(w_ordered).alias("created_at_period_max_weeks_since_created_at_cume_dist"),
        F.stddev_samp("weeks_since_created_at_count_15m").over(w_recent).alias("customer_id_stddev_samp_weeks_since_created_at_count_15m"),
        F.percent_rank().over(w_recent_2).alias("created_at_period_max_weeks_since_created_at_percent_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("created_at_period_max_weeks_since_created_at_cume_dist") >= 0.7, "LARGE").otherwise("SMALL").alias("created_at_period_max_weeks_since_created_at_cume_dist_tier"),
    )
    .agg(
        F.min("customer_id_stddev_samp_weeks_since_created_at_count_15m").alias("min_customer_id_stddev_samp_weeks_since_created_at_count_15m"),
        F.min("created_at_period_max_weeks_since_created_at_percent_rank").alias("min_created_at_period_max_weeks_since_created_at_percent_rank"),
        F.round(F.avg("weeks_since_created_at_cume_dist"), 2).alias("avg_weeks_since_created_at_cume_dist"),
        F.min("created_at_period_max_weeks_since_created_at_percent_rank").alias("min_created_at_period_max_weeks_since_created_at_percent_rank_2"),
        F.round(F.avg("created_at_period_max_weeks_since_created_at_cume_dist"), 2).alias("avg_created_at_period_max_weeks_since_created_at_cume_dist"),
        F.percentile_approx("created_at_period_max_weeks_since_created_at_cume_dist", 0.5).alias("created_at_period_max_weeks_since_created_at_cume_dist_p50"),
    )
    .orderBy(
        F.col("avg_created_at_period_max_weeks_since_created_at_cume_dist").desc_nulls_last(),
    )
)
