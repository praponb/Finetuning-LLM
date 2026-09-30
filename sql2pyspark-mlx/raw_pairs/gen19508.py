from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_running_2 = Window.partitionBy(F.month("effective_from_period"), "currency").orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_recent = Window.orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_hist = Window.orderBy(F.col("tier_value_max_7d").desc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

base = (
    exploded
    .filter(F.col("effective_from") >= F.lit("2024-06-10").cast("date"))
    .withColumn("effective_from_period", F.date_trunc("year", "effective_from"))
)

kept_keys = (
    base
    .select("currency")
    .intersectAll(
        df_price_tiers
        .filter(F.col("plan") == "basic")
        .select("currency")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.avg("tier_value").over(w).alias("currency_avg_tier_value"),
        F.max("tier_value").over(w2).alias("tier_value_max_7d"),
        F.count("tier_value").over(w3).alias("tier_value_count_30d"),
    )
)

scored = (
    metrics
    .filter(F.col("tier_value") <= 38)
    .select(
        "effective_from",
        "effective_from_period",
        "currency_avg_tier_value",
        "tier_value_max_7d",
        F.min("tier_value_max_7d").over(w).alias("currency_min_tier_value_max_7d"),
        F.count("tier_value").over(w_running_2).alias("tier_value_count_90d"),
        F.sum("tier_value").over(w).alias("currency_sum_tier_value"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.sum("tier_value_max_7d").over(w_recent).alias("tier_value_max_7d_sum_90d"),
        F.max("currency_min_tier_value_max_7d").over(w_recent).alias("currency_min_tier_value_max_7d_max_90d"),
        F.avg("currency_avg_tier_value").over(w_recent).alias("currency_avg_tier_value_avg_90d"),
    )
)

scored_price_tiers = (
    enriched
    .withColumn("tier_value_max_7d_sum_7d", F.sum("tier_value_max_7d").over(w2))
    .withColumn("tier_value_max_7d_max_7d", F.max("tier_value_max_7d").over(w2))
    .withColumn("tier_value_max_7d_cume_dist", F.cume_dist().over(w_hist))
    .withColumn("tier_value_max_7d_sum_90d_sum_7d", F.sum("tier_value_max_7d_sum_90d").over(w2))
)

result = (
    scored_price_tiers
    .groupBy(F.date_trunc("WEEK", "effective_from").alias("week_start"))
    .agg(
        F.count("*").alias("event_count"),
        F.min("tier_value_max_7d").alias("min_tier_value_max_7d"),
        ((F.unix_timestamp(F.max("effective_from_period")) - F.unix_timestamp(F.min("effective_from_period"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("span_minutes") > 0)
)
