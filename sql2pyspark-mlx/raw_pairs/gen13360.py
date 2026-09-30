from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.partitionBy(F.to_date("effective_from_period")).orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.orderBy("tier_value")
w4 = Window.orderBy(F.col("tier_value_percent_rank_filled").asc_nulls_last())

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_limits").alias("tier_value"))
)

valid = (
    flattened
    .filter(F.col("effective_from") >= F.lit("2024-02-19").cast("date"))
    .withColumn("effective_from_label", F.date_format("effective_from", "MMM yyyy"))
    .withColumn("tier_value_num", F.col("tier_value").cast("double"))
)

valid_price_tiers = (
    valid
    .filter(F.col("tier_value_num") >= 29)
    .select("*", F.date_trunc("YEAR", "effective_from").alias("effective_from_period"))
)

cleaned = (
    valid_price_tiers
    .filter(F.col("effective_from") > F.lit("2024-05-29").cast("date"))
    .select(
        "*",
        (F.datediff(F.current_date(), "effective_from_period") / 365).alias("years_since_effective_from_period"),
    )
)

with_windows = (
    cleaned
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w2))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w3))
)

base = (
    with_windows
    .filter(F.col("tier_value_percent_rank") < 0.3)
    .select(
        "id",
        "effective_from",
        "effective_from_label",
        "tier_value_num",
        "years_since_effective_from_period",
        "tier_value_percent_rank",
        "tier_value_cume_dist",
        "tier_value_dense_rank",
        F.coalesce("tier_value_percent_rank", F.lit(0)).alias("tier_value_percent_rank_filled"),
    )
)

with_windows_price_tiers = (
    base
    .filter(F.col("tier_value_dense_rank") != 3)
    .select(
        "effective_from",
        "effective_from_label",
        "tier_value_num",
        "tier_value_cume_dist",
        "tier_value_dense_rank",
        "tier_value_percent_rank_filled",
        F.percent_rank().over(w4).alias("tier_value_percent_rank_filled_percent_rank"),
    )
)

result = (
    with_windows_price_tiers
    .select(
        "tier_value_num",
        "tier_value_dense_rank",
        "tier_value_percent_rank_filled",
        "tier_value_percent_rank_filled_percent_rank",
    )
    .orderBy(F.col("tier_value_num").desc())
)
