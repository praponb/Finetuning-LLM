from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "JP") & ~F.col("email").like("%@startup.io"))
    .agg(
        F.count("age").alias("age_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.count("*").alias("total_count"),
    )
    .orderBy("total_count")
)
