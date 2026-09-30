from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("currency").orderBy(F.col("adjusted_tier_value").desc())
w3 = Window.partitionBy("currency").orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

base = (
    lines
    .filter(F.col("tier_value") < 22)
    .select(
        "*",
        F.when(F.col("currency").isin(["USD", "EUR"]), "Core").otherwise("Other").alias("currency_group"),
    )
)

base_price_tiers = (
    base
    .filter(F.col("currency").isin(["EUR", "THB"]))
    .select("*", F.col("tier_value").cast("string").alias("tier_value_num"))
)

valid = (
    base_price_tiers
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-01-25"))) & (F.col("tier_value") > 31))
    .select(
        "*",
        F.round((F.col("tier_value") * 0.85), 2).alias("adjusted_tier_value"),
        F.date_trunc("month", "effective_from").alias("effective_from_period"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.count("tier_value").over(w).alias("tier_value_count_90d"),
        F.dense_rank().over(w2).alias("adjusted_tier_value_dense_rank"),
        F.rank().over(w2).alias("adjusted_tier_value_rank"),
        F.max("adjusted_tier_value").over(w3).alias("adjusted_tier_value_max_30d"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("adjusted_tier_value_dense_rank") >= 8, "LARGE").otherwise("SMALL").alias("adjusted_tier_value_dense_rank_tier"),
        F.date_trunc("MONTH", "effective_from_period").alias("activity_month"),
    )
    .agg(
        F.max(F.when(F.col("currency") == "EUR", F.col("adjusted_tier_value_dense_rank"))).alias("eur_max_adjusted_tier_value_dense_rank"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.min("effective_from_period").alias("started_at"),
    )
)
