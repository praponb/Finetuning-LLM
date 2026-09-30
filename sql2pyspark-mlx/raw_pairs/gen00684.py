from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w_running = Window.partitionBy("currency")
w_hist = Window.orderBy(F.col("val").asc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("val"),
    )
)

cleaned = (
    exploded
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-05-30"))) & (F.col("currency") == "USD"))
    .select("*", F.lower("plan").alias("plan_lower"))
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("val") == 61)
    .select(
        "*",
        F.concat("plan", F.lit(", "), "currency").alias("plan_currency"),
        F.concat("plan", F.lit(", "), "currency").alias("plan_currency_2"),
    )
)

filtered = (
    cleaned_price_tiers
    .filter(F.col("effective_from") >= F.lit("2024-03-11").cast("date"))
    .withColumn("currency_group", F.when(F.col("currency").isin(["USD", "EUR"]), "Tier 1").otherwise("Other"))
)

kept_keys = (
    filtered
    .select("currency")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from") > "2024-02-01")
        .select("currency")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

scored = (
    verified
    .withColumn("plan_max_val", F.max("val").over(w))
    .withColumn("currency_max_val", F.max("val").over(w_running))
    .withColumn("val_percent_rank", F.percent_rank().over(w_hist))
)

result = (
    scored
    .groupBy("currency")
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.percentile_approx("currency_max_val", [0.25, 0.5, 0.75]).alias("currency_max_val_quartiles"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.round(F.avg("currency_max_val"), 2).alias("avg_currency_max_val"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("enterprise_count").desc())
)
