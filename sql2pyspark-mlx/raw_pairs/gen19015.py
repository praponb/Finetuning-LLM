from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency").orderBy(F.col("is_usd").desc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("tier_value") < 28)
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.when(F.col("currency") == "USD", 1).otherwise(0).alias("is_usd"),
        F.initcap(F.trim("currency")).alias("currency_title"),
    )
)

with_windows = cleaned.select("*", F.cume_dist().over(w).alias("is_usd_cume_dist"))

base = (
    with_windows
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-05-21")))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "is_usd",
        "currency_title",
        "is_usd_cume_dist",
        F.when(F.col("is_usd") >= 66, "High").when(F.col("is_usd") >= 23, "Medium").otherwise("Low").alias("is_usd_band"),
        F.when(F.col("currency").isin(["USD", "THB"]), "Priority").otherwise("Other").alias("currency_group"),
    )
)

result = (
    base
    .select(
        "id",
        "plan",
        "effective_from",
        "currency_title",
        "is_usd_cume_dist",
        "is_usd_band",
    )
)
