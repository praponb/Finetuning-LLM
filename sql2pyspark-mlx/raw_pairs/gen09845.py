from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "SG") | F.col("supplier_name").like("%Par%"))
    .agg(
        F.max("rating").alias("max_rating"),
        F.count("rating").alias("rating_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
    .orderBy(F.col("rating_count").desc())
)
