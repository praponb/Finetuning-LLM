from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        "city",
        F.when(F.col("age") >= 58, "high").when(F.col("age") >= 27, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.max("age").alias("max_age"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
)
