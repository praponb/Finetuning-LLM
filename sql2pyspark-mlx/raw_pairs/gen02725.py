from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("plan")

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("val"))
)

with_windows = (
    exploded
    .filter(F.col("plan").isin(["starter", "basic", "enterprise"]))
    .withColumn("currency_avg_val", F.avg("val").over(w))
    .withColumn("plan_stddev_samp_val", F.stddev_samp("val").over(w2))
)

valid = (
    with_windows
    .filter(F.col("plan").isin(["enterprise", "basic"]))
    .select(
        "plan",
        "currency",
        "val",
        "currency_avg_val",
        "plan_stddev_samp_val",
        F.round("plan_stddev_samp_val", 2).alias("plan_stddev_samp_val_rounded"),
    )
)

base = (
    valid
    .filter(F.col("plan_stddev_samp_val") != 72)
    .withColumn("plan_stddev_samp_val_tier", F.when(F.col("plan_stddev_samp_val") >= 71, "High").when(F.col("plan_stddev_samp_val") >= 28, "Medium").otherwise("Low"))
)

daily = (
    base
    .groupBy("currency")
    .agg(
        F.round(F.sum("plan_stddev_samp_val_rounded") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("plan_stddev_samp_val_rounded_per_usd"),
        F.min("plan_stddev_samp_val").alias("min_plan_stddev_samp_val"),
        F.count("*").alias("row_count"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("min_plan_stddev_samp_val") < 1000000)
)

result = (
    daily
    .select(
        "currency",
        "min_plan_stddev_samp_val",
        "num_rows",
        F.when(F.col("currency").rlike("^(THB|EUR)$"), "EXTENDED").when(F.col("currency").rlike("^(USD)$"), "OTHER_ZONE").otherwise("CORE").alias("currency_zone"),
    )
    .orderBy(F.col("min_plan_stddev_samp_val").desc_nulls_last())
)
