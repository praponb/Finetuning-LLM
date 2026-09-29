from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 3.4) | (F.col("country") != "SG"))
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.min("rating").alias("min_rating"),
    )
    .filter(F.col("min_rating") >= 3.8)
    .orderBy("city")
    .limit(10)
)
