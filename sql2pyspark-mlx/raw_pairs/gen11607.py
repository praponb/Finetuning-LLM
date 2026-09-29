from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w_running = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("days_since_starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("quarter_start").orderBy(F.col("num_channels").desc())

filtered = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-08-26").cast("date"))
    .withColumn("days_since_starts_on", F.datediff(F.current_date(), "starts_on"))
)

latest = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    latest
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.count("days_since_starts_on").over(w_running).alias("running_count_days_since_starts_on"),
        F.sum("days_since_starts_on").over(w_running).alias("running_sum_days_since_starts_on"),
        F.avg("days_since_starts_on").over(w_running).alias("running_avg_days_since_starts_on"),
        F.min("days_since_starts_on").over(w_running).alias("running_min_days_since_starts_on"),
        F.last("days_since_starts_on", ignorenulls=True).over(w3).alias("last_known_days_since_starts_on"),
    )
)

with_window = (
    enriched
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

totals = (
    with_window
    .groupBy(F.date_trunc("quarter", "ends_on").alias("quarter_start"), "promo_id")
    .agg(
        F.max("running_min_days_since_starts_on").alias("max_running_min_days_since_starts_on"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("discount_pct_per_partner"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.round(F.avg("last_known_days_since_starts_on"), 2).alias("avg_last_known_days_since_starts_on"),
        F.count(F.when(F.col("promotion_promo_code") == "VIP", 1)).alias("vip_count"),
    )
    .withColumn("num_channels_dense_rank", F.dense_rank().over(w4))
)

result = (
    totals
    .select(
        "quarter_start",
        "promo_id",
        "max_running_min_days_since_starts_on",
        "num_channels",
        "vip_count",
        "num_channels_dense_rank",
    )
    .orderBy(F.col("quarter_start").desc_nulls_last())
)
