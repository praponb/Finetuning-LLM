from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc_nulls_first())
w2 = Window.partitionBy(F.month("effective_from"), "plan")
w3 = Window.partitionBy("plan").orderBy(F.col("val").desc())
w4 = Window.orderBy("val")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("val"),
    )
)

scored = (
    flattened
    .filter(F.col("currency").isin(["EUR", "USD", "THB"]))
    .select(
        "*",
        F.cume_dist().over(w).alias("val_cume_dist"),
        F.stddev_samp("val").over(w2).alias("effective_from_month_stddev_samp_val"),
        F.cume_dist().over(w3).alias("val_cume_dist_2"),
        F.rank().over(w4).alias("val_rank"),
    )
)

remaining_keys = (
    scored
    .select("currency", "plan")
    .intersect(
        df_price_tiers
        .filter(F.col("effective_from") >= "2024-07-06")
        .select("currency", "plan")
    )
)

verified = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("effective_from") >= F.lit("2024-01-27").cast("date"))
    .select(
        "*",
        F.initcap(F.trim("currency")).alias("currency_title"),
        (F.col("val_rank") * F.col("effective_from_month_stddev_samp_val")).alias("val_rank_times_effective_from_month_stddev_samp_val"),
    )
)

result = (
    valid
    .groupBy(
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
        F.date_trunc("YEAR", "effective_from").alias("year_start"),
    )
    .agg(
        F.countDistinct("currency").alias("distinct_currencies"),
        F.percentile_approx("val_cume_dist_2", [0.25, 0.5, 0.75]).alias("val_cume_dist_2_quartiles"),
        F.percentile_approx("val_cume_dist", [0.25, 0.5, 0.75]).alias("val_cume_dist_quartiles"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
    )
    .filter(F.col("usd_count") >= 1)
)
