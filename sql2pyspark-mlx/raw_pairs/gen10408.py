from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("days_since_effective_from").asc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

cleaned = (
    exploded
    .filter(~F.col("currency").isin(["THB", "USD"]))
    .withColumn("plan_length", F.length("plan"))
    .withColumn("days_since_effective_from", F.datediff(F.current_date(), "effective_from"))
)

base = (
    cleaned
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-05-23")))
    .select(
        "effective_from",
        "currency",
        "days_since_effective_from",
        (F.col("days_since_effective_from") / F.nullif("tier_value", F.lit(0))).alias("days_since_effective_from_per_tier_value"),
        F.upper("plan").alias("plan_upper"),
    )
)

scored = (
    base
    .select("*", F.dense_rank().over(w).alias("days_since_effective_from_dense_rank"))
)

result = (
    scored
    .select("effective_from", "currency", "days_since_effective_from_dense_rank")
    .orderBy(
        F.col("effective_from").asc_nulls_last(),
        F.col("days_since_effective_from_dense_rank").desc(),
    )
)
