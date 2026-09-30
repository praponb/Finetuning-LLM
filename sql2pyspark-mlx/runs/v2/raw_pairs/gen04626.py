from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "US")
    .groupBy("country")
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.max("rating").alias("max_rating"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
    .orderBy("country")
    .limit(3)
)
