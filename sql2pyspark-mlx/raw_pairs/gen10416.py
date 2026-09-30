from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

result = (
    lines
    .withColumn("val_tier", F.when(F.col("val") >= 69, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("MONTH", "effective_from"))
    .groupingSets(
        [["val_tier", "activity_month"], ["val_tier"]],
        "val_tier",
        "activity_month",
    )
    .agg(
        F.round(F.avg("val"), 2).alias("avg_val"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
    )
    .orderBy(F.col("avg_val").desc())
)
