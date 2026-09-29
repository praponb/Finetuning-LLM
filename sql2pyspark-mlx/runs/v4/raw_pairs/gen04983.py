from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    flattened
    .withColumn("quarter_start", F.date_trunc("QUARTER", "effective_from"))
    .groupingSets(
        [["quarter_start", "plan"], ["quarter_start"]],
        "quarter_start",
        "plan",
    )
    .agg(
        F.sum("tier_value").alias("total_tier_value"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
        F.avg("tier_value").alias("avg_tier_value"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.grouping("quarter_start").alias("grouping_id"),
    )
    .orderBy("event_count")
)
