from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.partitionBy("plan").orderBy("tier_value_dense_rank")
w3 = Window.partitionBy(F.date_trunc("month", "effective_from_period"), "plan").orderBy(F.col("effective_from_year").asc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

base = (
    exploded
    .filter(F.col("effective_from") < "2025-09-01")
    .select("*", F.last_day("effective_from").alias("effective_from_month_end"))
)

base_price_tiers = (
    base
    .filter((F.col("effective_from") > F.lit("2024-04-14").cast("date")) & (F.col("tier_value") > 32))
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value",
        F.date_format("effective_from", "MMM yyyy").alias("effective_from_label"),
    )
)

cleaned = (
    base_price_tiers
    .filter(F.col("tier_value") >= 21)
    .select(
        "*",
        F.date_trunc("MONTH", "effective_from").alias("effective_from_period"),
        F.year("effective_from").alias("effective_from_year"),
    )
)

metrics = cleaned.withColumn("tier_value_dense_rank", F.dense_rank().over(w))

base_price_tiers_2 = (
    metrics
    .filter(F.col("effective_from") <= "2025-09-01")
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value",
        "effective_from_label",
        "effective_from_period",
        "effective_from_year",
        "tier_value_dense_rank",
        F.date_trunc("YEAR", "effective_from_period").alias("effective_from_period_period"),
    )
)

scored = (
    base_price_tiers_2
    .filter(F.col("tier_value") <= 61)
    .select(
        "*",
        F.percent_rank().over(w2).alias("tier_value_dense_rank_percent_rank"),
        F.dense_rank().over(w3).alias("effective_from_year_dense_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "plan",
        "effective_from",
        "effective_from_label",
        "effective_from_period_period",
        "tier_value_dense_rank_percent_rank",
        "effective_from_year_dense_rank",
    )
    .orderBy("effective_from_year_dense_rank", "effective_from")
)
