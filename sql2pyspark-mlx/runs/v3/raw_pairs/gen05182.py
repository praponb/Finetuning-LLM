from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "DE")
    .groupBy("city")
    .agg(
        F.max("rating").alias("max_rating"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.min("rating").alias("min_rating"),
    )
    .filter(F.col("max_rating") >= 4.0)
    .orderBy("city")
)
