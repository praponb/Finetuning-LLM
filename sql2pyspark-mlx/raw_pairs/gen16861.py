from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").desc())
w_hist = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

valid = (
    exploded
    .filter(F.col("effective_from") > F.lit("2024-03-21").cast("date"))
    .select(
        "*",
        (F.datediff(F.current_date(), "effective_from") / 30).alias("months_since_effective_from"),
        F.length("plan").alias("plan_length"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "plan",
        "effective_from",
        "months_since_effective_from",
        F.rank().over(w).alias("val_rank"),
        F.stddev_samp("months_since_effective_from").over(w_hist).alias("currency_stddev_samp_months_since_effective_from"),
        F.sum("val").over(w_hist).alias("currency_sum_val"),
    )
)

filtered = (
    metrics
    .filter(F.col("currency_sum_val") >= 327)
    .select(
        "id",
        "plan",
        "val_rank",
        "currency_stddev_samp_months_since_effective_from",
        F.coalesce("currency_sum_val", F.lit(0)).alias("currency_sum_val_filled"),
        (F.col("currency_stddev_samp_months_since_effective_from") - F.col("months_since_effective_from")).alias("currency_stddev_samp_months_since_effective_from_minus_months_since_effective_from"),
    )
)

matched_keys = (
    filtered
    .select("plan", "id")
    .subtract(
        df_price_tiers
        .filter(F.col("plan") == "starter")
        .select("plan", "id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("currency_stddev_samp_months_since_effective_from").between(61, 66))
    .select(
        "*",
        F.col("currency_stddev_samp_months_since_effective_from").cast("string").alias("currency_stddev_samp_months_since_effective_from_num"),
        F.lower("plan").alias("plan_lower"),
    )
)

result = (
    base
    .groupBy("plan")
    .agg(
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
        F.max(F.when(F.col("plan") == "starter", F.col("val_rank"))).alias("starter_max_val_rank"),
    )
    .orderBy(F.col("starter_max_val_rank").desc())
)
