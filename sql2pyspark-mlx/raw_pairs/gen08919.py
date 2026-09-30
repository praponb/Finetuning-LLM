from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

result = (
    exploded
    .withColumn("week_start", F.date_trunc("WEEK", "effective_from"))
    .rollup("week_start", "plan")
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.count("*").alias("num_rows"),
        F.round(F.sum("tier_value") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("tier_value_per_enterprise"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows_2"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.grouping("week_start").alias("grouping_id"),
    )
    .orderBy(F.col("basic_rows").desc())
)
