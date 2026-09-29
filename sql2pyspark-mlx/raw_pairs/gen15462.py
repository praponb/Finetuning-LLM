from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("val"))
)

result = (
    lines
    .groupBy(F.when(F.col("val") >= 87, "HIGH").otherwise("LOW").alias("val_tier"))
    .agg(
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.count_if(F.col("currency") == "THB").alias("thb_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("val"))).alias("enterprise_total_val"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("num_rows").asc_nulls_first())
)
