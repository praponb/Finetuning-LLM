from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("email").like("%@startup.io"))
    .groupBy("segment")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.min("age").alias("min_age"),
    )
    .filter(F.col("min_age") > 58)
    .orderBy("country_values", "segment")
    .limit(5)
)
