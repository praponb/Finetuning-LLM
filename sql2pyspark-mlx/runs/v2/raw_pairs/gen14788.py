from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() & F.col("supplier_name").like("%City%"))
    .groupBy("city")
    .agg(
        F.max("rating").alias("max_rating"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
    .filter(F.col("max_rating") < 3.1)
    .orderBy("city_values")
)
