from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy("is_active")
    .agg(
        F.min("age").alias("min_age"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
    .filter(F.col("min_age") < 59)
    .orderBy(F.col("city_values").desc())
)
