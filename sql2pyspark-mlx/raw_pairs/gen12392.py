from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").desc())
w2 = Window.partitionBy("val_filled_tier").orderBy(F.col("val_filled_p95").desc())

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("discounts").alias("val"))
)

filtered = (
    lines
    .filter(F.col("effective_from") > F.lit("2024-06-15").cast("date"))
    .select(
        "*",
        F.lower("plan").alias("plan_lower"),
        F.coalesce("val", F.lit(0)).alias("val_filled"),
    )
)

enriched = (
    filtered
    .filter(F.month("effective_from").isin([4, 5, 9]))
    .select(
        "plan",
        "val",
        "plan_lower",
        "val_filled",
        F.cume_dist().over(w).alias("val_cume_dist"),
    )
)

cleaned = (
    enriched
    .filter(F.col("val") <= 47)
    .select(
        "val",
        "plan_lower",
        "val_filled",
        F.round("val", 0).alias("val_rounded"),
        (F.col("val_cume_dist") / F.nullif("val", F.lit(0))).alias("val_cume_dist_per_val"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("val_filled") >= 67, "HIGH").otherwise("LOW").alias("val_filled_tier"),
        F.when(F.col("val") >= 63, "PREMIUM").otherwise("STANDARD").alias("val_tier"),
    )
    .agg(
        F.avg("val_filled").alias("avg_val_filled"),
        F.percentile_approx("val_filled", 0.95).alias("val_filled_p95"),
    )
    .withColumn("val_filled_p95_rank", F.rank().over(w2))
    .orderBy("avg_val_filled")
)
