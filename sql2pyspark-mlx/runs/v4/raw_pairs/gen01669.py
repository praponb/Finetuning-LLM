from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 12357, "LARGE").otherwise("SMALL").alias("sqft_tier"),
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
    )
    .agg(
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.max("sqft").alias("max_sqft"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .orderBy(F.col("avg_sqft").desc_nulls_last())
)
