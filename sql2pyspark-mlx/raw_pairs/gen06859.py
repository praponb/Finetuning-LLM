from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 195, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
