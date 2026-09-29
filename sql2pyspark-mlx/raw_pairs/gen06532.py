from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

result = (
    exploded
    .groupBy(F.when(F.col("val") >= 62, "LARGE").otherwise("SMALL").alias("val_tier"))
    .agg(
        F.percentile_approx("val", 0.95).alias("val_p95"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.max("val").alias("max_val"),
    )
    .orderBy(F.col("num_currencies").asc_nulls_last())
)
