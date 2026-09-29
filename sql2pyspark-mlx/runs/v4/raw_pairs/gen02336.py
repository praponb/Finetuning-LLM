from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("val"))
)

result = (
    lines
    .groupBy("plan", "currency")
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.percentile_approx("val", 0.9).alias("val_p90"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("val")).otherwise(0)).alias("enterprise_val"),
        F.percentile_approx("val", 0.95).alias("val_p95"),
    )
)
