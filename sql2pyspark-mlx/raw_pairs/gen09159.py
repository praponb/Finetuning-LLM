from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.partitionBy(F.date_trunc("week", "effective_from"), "plan").orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy(F.month("effective_from"), "plan")
w5 = Window.partitionBy(F.month("effective_from"), "plan").orderBy(F.col("tier_value_max_7d").asc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_prices").alias("tier_value"),
    )
)

base = (
    lines
    .filter(F.col("plan") == "pro")
    .select("*", F.date_trunc("YEAR", "effective_from").alias("effective_from_period"))
)

with_windows = (
    base
    .select(
        "plan",
        "effective_from",
        "tier_value",
        F.max("tier_value").over(w).alias("plan_max_tier_value"),
        F.rank().over(w2).alias("tier_value_rank"),
        F.max("tier_value").over(w3).alias("tier_value_max_7d"),
        F.min("tier_value").over(w4).alias("effective_from_month_min_tier_value"),
    )
)

valid = (
    with_windows
    .filter((F.col("effective_from") > F.lit("2024-03-03").cast("date")) & (F.col("plan") == "basic"))
    .withColumn("effective_from_month_min_tier_value_whole", F.floor("effective_from_month_min_tier_value"))
    .withColumn("tier_value_times_effective_from_month_min_tier_value", (F.col("tier_value") * F.col("effective_from_month_min_tier_value")))
)

scored = (
    valid
    .filter(F.col("tier_value_rank") > 5)
    .select(
        "plan",
        "tier_value_rank",
        "tier_value_max_7d",
        "effective_from_month_min_tier_value",
        "effective_from_month_min_tier_value_whole",
        "tier_value_times_effective_from_month_min_tier_value",
        F.percent_rank().over(w5).alias("tier_value_max_7d_percent_rank"),
    )
)

result = (
    scored
    .select(
        "plan",
        "tier_value_max_7d_percent_rank",
        "tier_value_times_effective_from_month_min_tier_value",
    )
    .unpivot(
        ids=["plan"],
        values=["tier_value_max_7d_percent_rank", "tier_value_times_effective_from_month_min_tier_value"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
