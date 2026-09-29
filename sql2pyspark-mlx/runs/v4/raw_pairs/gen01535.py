from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("val"),
    )
)

result = (
    lines
    .groupBy(
        "currency",
        F.when(F.col("val") >= 84, "PREMIUM").otherwise("STANDARD").alias("val_tier"),
    )
    .agg(
        F.percentile_approx("val", 0.9).alias("val_p90"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sort_array(F.collect_set("plan")).alias("plan_list_2"),
        F.min("val").alias("min_val"),
    )
)
