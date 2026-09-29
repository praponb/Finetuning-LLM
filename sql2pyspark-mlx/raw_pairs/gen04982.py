from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.count_if(F.col("city") == "Berlin").alias("berlin_events"),
        F.countDistinct("region").alias("distinct_regions"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
    )
    .filter(F.col("sqft_stddev") > 0)
)
