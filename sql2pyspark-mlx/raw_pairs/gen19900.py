from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_limits").alias("element"),
    )
)

result = (
    exploded
    .rollup(
        F.when(F.col("element") >= 79, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
        "plan",
    )
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .orderBy(F.col("enterprise_events").desc_nulls_first())
)
