from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "currency", F.explode("discounts").alias("val"))

result = (
    flattened
    .groupBy(
        F.when(F.col("val") >= 90, "LARGE").otherwise("SMALL").alias("val_tier"),
        "currency",
    )
    .agg(
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("val", 0.9).alias("val_p90"),
    )
    .filter(F.col("val_p90") > 0)
    .orderBy(F.col("val_p90").desc())
)
