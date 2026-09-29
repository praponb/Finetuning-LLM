from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", F.explode_outer("tier_prices").alias("element"))

result = (
    exploded
    .groupBy(
        F.when(F.col("element") >= 68, "LARGE").otherwise("SMALL").alias("element_tier"),
    )
    .agg(
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") >= 1)
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
