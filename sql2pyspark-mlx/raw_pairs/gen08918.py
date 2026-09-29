from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

result = (
    lines
    .cube(F.date_trunc("WEEK", "effective_from").alias("week_start"), "plan")
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.sum(F.when(F.col("plan") == "basic", F.col("tier_value"))).alias("basic_total_tier_value"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
    )
    .filter(F.col("basic_rows") < 1000)
    .orderBy(F.col("basic_events").asc_nulls_first())
)
