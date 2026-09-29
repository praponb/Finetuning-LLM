from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.min("rating").alias("min_rating"),
    )
    .filter(F.col("min_rating") > 4.0)
    .select("country", "country_values", "min_rating")
    .orderBy("country")
    .limit(20)
)
