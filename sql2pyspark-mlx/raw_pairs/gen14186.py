from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.when(F.col("units") >= 32, "HIGH").otherwise("LOW").alias("units_tier"))
    .agg(
        F.bit_and("units").alias("units_bit_and"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.stddev_samp("revenue").alias("revenue_stddev_2"),
    )
    .orderBy(F.col("units_bit_and").desc())
)
