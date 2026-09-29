from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_first())
w_hist = Window.partitionBy(F.date_trunc("WEEK", "effective_from"), "plan")
w3 = Window.partitionBy(F.date_trunc("week", "effective_from"))
w4 = Window.partitionBy("plan")
w5 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w_grp = Window.partitionBy("plan").orderBy(F.col("tier_value_percent_rank").desc_nulls_last())
w7 = Window.orderBy(F.col("tier_value").desc_nulls_last())
w_ordered = Window.orderBy(F.col("tier_value_percent_rank_rank").desc_nulls_last())
w_part = Window.orderBy(F.col("tier_value_rank").desc_nulls_last())
w10 = Window.orderBy(F.col("tier_value_percent_rank_rank").desc())
w11 = Window.orderBy(F.col("tier_value_percent_rank").asc_nulls_first())
w12 = Window.orderBy(F.col("tier_value_percent_rank_rank_percent_rank_2_int").desc())
w13 = Window.orderBy(F.col("tier_value_rank_dense_rank").desc())
w14 = Window.orderBy(F.col("tier_value_percent_rank_rank_2").desc())
w15 = Window.orderBy(F.col("tier_value_percent_rank_rank_percent_rank").asc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_prices").alias("tier_value"),
    )
)

kept_keys = (
    flattened
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.col("currency") != "THB")
        .select("id")
    )
)

kept_rows = (
    flattened
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .filter(F.col("tier_value") >= 67)
    .select(
        "*",
        F.rank().over(w).alias("tier_value_rank"),
        F.count("tier_value").over(w_hist).alias("effective_from_period_count_tier_value"),
        F.sum("tier_value").over(w3).alias("effective_from_period_sum_tier_value"),
        F.max("tier_value").over(w4).alias("plan_max_tier_value"),
        F.percent_rank().over(w5).alias("tier_value_percent_rank"),
    )
)

enriched = (
    with_windows
    .select(
        "tier_value_rank",
        "tier_value_percent_rank",
        F.rank().over(w_grp).alias("tier_value_percent_rank_rank"),
        F.cume_dist().over(w7).alias("tier_value_cume_dist"),
        F.min("plan_max_tier_value").over(w4).alias("plan_min_plan_max_tier_value"),
    )
)

metrics = (
    enriched
    .select(
        "tier_value_rank",
        "tier_value_percent_rank_rank",
        F.dense_rank().over(w_ordered).alias("tier_value_percent_rank_rank_dense_rank"),
        F.dense_rank().over(w_part).alias("tier_value_rank_dense_rank"),
        F.percent_rank().over(w10).alias("tier_value_percent_rank_rank_percent_rank"),
        F.rank().over(w11).alias("tier_value_percent_rank_rank_2"),
        F.percent_rank().over(w_ordered).alias("tier_value_percent_rank_rank_percent_rank_2"),
    )
)

cleaned = (
    metrics
    .filter(F.col("tier_value_percent_rank_rank") >= 4)
    .select(
        "*",
        F.col("tier_value_percent_rank_rank_percent_rank_2").cast("int").alias("tier_value_percent_rank_rank_percent_rank_2_int"),
        F.col("tier_value_percent_rank_rank_percent_rank_2").cast("int").alias("tier_value_percent_rank_rank_percent_rank_2_int_2"),
    )
)

with_windows_price_tiers = (
    cleaned
    .withColumn("tier_value_percent_rank_rank_percent_rank_2_int_cume_dist", F.cume_dist().over(w12))
    .withColumn("tier_value_rank_dense_rank_percent_rank", F.percent_rank().over(w13))
    .withColumn("tier_value_rank_rank", F.rank().over(w_part))
    .withColumn("tier_value_percent_rank_rank_2_rank", F.rank().over(w14))
    .withColumn("tier_value_percent_rank_rank_percent_rank_rank", F.rank().over(w15))
)

result = (
    with_windows_price_tiers
    .select(
        "tier_value_percent_rank_rank",
        "tier_value_percent_rank_rank_percent_rank",
        "tier_value_percent_rank_rank_2",
        "tier_value_percent_rank_rank_percent_rank_2_int",
        "tier_value_percent_rank_rank_percent_rank_2_int_2",
        "tier_value_percent_rank_rank_percent_rank_2_int_cume_dist",
        "tier_value_rank_dense_rank_percent_rank",
        "tier_value_rank_rank",
    )
)
