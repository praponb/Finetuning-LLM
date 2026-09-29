from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "plan", F.explode_outer("tier_limits").alias("val"))

result = (
    lines
    .groupBy("plan")
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("val_per_starter"),
        F.sum("val").alias("total_val"),
    )
)
