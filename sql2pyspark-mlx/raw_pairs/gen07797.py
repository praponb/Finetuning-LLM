from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("val"))
)

result = (
    exploded
    .cube(
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
        F.when(F.col("val") >= 64, "HIGH").otherwise("LOW").alias("val_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("val", 0.5).alias("val_p50"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
    .orderBy(F.col("distinct_plans").desc())
)
