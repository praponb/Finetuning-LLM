from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") != 32)
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.min("age").alias("min_age"),
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
    )
    .filter(F.col("min_age") < 40)
)
