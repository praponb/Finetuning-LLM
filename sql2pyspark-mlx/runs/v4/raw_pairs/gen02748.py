from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.5, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.count_if(F.col("city") == "Singapore").alias("singapore_events"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("rating_tier").asc_nulls_first())
)
