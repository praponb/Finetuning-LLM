from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("effective_from"), "plan").orderBy(F.col("tier_value").desc_nulls_last())
w_part = Window.partitionBy("plan", "currency")
w3 = Window.partitionBy(F.date_trunc("WEEK", "effective_from"), "plan").orderBy(F.col("plan_sum_tier_value").desc_nulls_last())
w4 = Window.partitionBy("currency").orderBy("tier_value_rank")
w5 = Window.partitionBy(F.month("effective_from"), "plan")
w6 = Window.partitionBy(F.month("effective_from")).orderBy(F.col("tier_value").asc_nulls_first())
w7 = Window.partitionBy(F.date_trunc("WEEK", "effective_from"))

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

kept_keys = (
    flattened
    .select("plan", "currency")
    .intersect(
        df_price_tiers
        .filter(F.col("plan") != "starter")
        .select("plan", "currency")
    )
)

kept_rows = (
    flattened
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

remaining_keys = (
    kept_rows
    .select("currency")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from").between("2023-05-01", "2023-12-25"))
        .select("currency")
    )
)

kept_rows_price_tiers = (
    kept_rows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows_price_tiers
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-02-24"))) & (F.col("tier_value") > 31))
    .withColumn("currency_upper", F.upper("currency"))
)

scored = (
    cleaned
    .withColumn("tier_value_rank", F.rank().over(w))
    .withColumn("plan_sum_tier_value", F.sum("tier_value").over(w_part))
)

enriched = (
    scored
    .filter(F.col("plan_sum_tier_value") != 380)
    .select(
        "id",
        "plan",
        F.percent_rank().over(w3).alias("plan_sum_tier_value_percent_rank"),
        F.dense_rank().over(w4).alias("tier_value_rank_dense_rank"),
        F.stddev_samp("tier_value_rank").over(w5).alias("effective_from_month_stddev_samp_tier_value_rank"),
        F.cume_dist().over(w6).alias("tier_value_cume_dist"),
        F.count("tier_value").over(w7).alias("effective_from_period_count_tier_value"),
    )
)

result = (
    enriched
    .select(
        "plan",
        "plan_sum_tier_value_percent_rank",
        "effective_from_month_stddev_samp_tier_value_rank",
    )
)
