from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Eu%"))
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.avg("rating").alias("avg_rating"),
    )
    .filter(F.col("avg_rating") < 2.5)
    .orderBy("city_values")
)
