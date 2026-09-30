from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.stddev_samp("units").alias("units_stddev"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("avg_units")
)
