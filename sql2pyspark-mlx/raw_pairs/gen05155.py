from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("val"))

result = (
    exploded
    .groupBy(
        F.when(F.col("val") >= 86, "HIGH").otherwise("LOW").alias("val_tier"),
        "plan",
    )
    .agg(
        F.min("val").alias("min_val"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
)
