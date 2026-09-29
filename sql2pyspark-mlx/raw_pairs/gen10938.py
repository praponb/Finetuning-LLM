from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

result = (
    lines
    .withColumn("element_tier", F.when(F.col("element") >= 81, "PREMIUM").otherwise("STANDARD"))
    .cube("element_tier")
    .agg(
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.min("element").alias("min_element"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("element_per_pro"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.percentile_approx("element", 0.95).alias("element_p95"),
        F.grouping("element_tier").alias("grouping_id"),
    )
    .orderBy(F.col("num_currencies").asc_nulls_first())
)
