from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("month", "effective_from"), "plan").orderBy(F.col("tier_value").desc())
w3 = Window.partitionBy("plan")
w4 = Window.orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy(F.year("effective_from_period"))

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

filtered = (
    exploded
    .filter((F.col("effective_from") > F.lit("2024-05-11").cast("date")) & (F.col("plan") == "starter"))
    .select("*", F.date_trunc("WEEK", "effective_from").alias("effective_from_period"))
)

metrics = (
    filtered
    .filter(F.col("effective_from").between("2023-07-01", "2024-04-01"))
    .select(
        "tier_value",
        "effective_from_period",
        F.max("tier_value").over(w).alias("tier_value_max_90d"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
        F.sum("tier_value").over(w3).alias("plan_sum_tier_value"),
    )
)

cleaned = (
    metrics
    .filter(F.to_date("effective_from_period") >= "2024-02-01")
    .select(
        "*",
        F.date_trunc("WEEK", "effective_from_period").alias("effective_from_period_period"),
    )
)

metrics_price_tiers = (
    cleaned
    .select(
        "tier_value",
        "tier_value_max_90d",
        "tier_value_percent_rank",
        "plan_sum_tier_value",
        F.count("plan_sum_tier_value").over(w4).alias("plan_sum_tier_value_count_30d"),
        F.stddev_samp("tier_value_percent_rank").over(w5).alias("effective_from_period_year_stddev_samp_tier_value_percent_rank"),
    )
)

result = (
    metrics_price_tiers
    .select(
        "tier_value_max_90d",
        "tier_value_percent_rank",
        "plan_sum_tier_value",
        "plan_sum_tier_value_count_30d",
    )
    .orderBy("plan_sum_tier_value", "plan_sum_tier_value_count_30d")
)
