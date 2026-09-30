from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_prices").alias("val"))
)

result = (
    flattened
    .groupBy("plan")
    .agg(
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
        F.avg("val").alias("avg_val"),
        F.max(F.when(F.col("plan") == "enterprise", F.col("val"))).alias("enterprise_max_val"),
    )
)
