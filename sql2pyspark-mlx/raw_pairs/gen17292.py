from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency")
w2 = Window.partitionBy("currency", "plan")

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

filtered = (
    exploded
    .filter(F.col("effective_from") > F.lit("2024-01-31").cast("date"))
    .withColumn("currency_plan_key", F.concat_ws("-", "currency", "plan"))
)

with_windows = (
    filtered
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.count("val").over(w).alias("plan_count_val"),
        F.min("val").over(w2).alias("currency_min_val"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("effective_from") <= "2023-03-01")
    .select("*", F.substring("currency", 1, 2).alias("currency_prefix"))
)

matched_keys = (
    cleaned
    .select("plan", "id")
    .exceptAll(
        df_price_tiers
        .filter(F.month("effective_from").isin([4, 5, 6]))
        .select("plan", "id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("QUARTER", "effective_from").alias("quarter_start"))
    .agg(
        F.percentile_approx("currency_min_val", 0.95).alias("currency_min_val_p95"),
        F.avg(F.when(F.col("plan") == "pro", F.col("plan_count_val"))).alias("pro_avg_plan_count_val"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
    )
    .orderBy(F.col("enterprise_rows").desc_nulls_last())
)
