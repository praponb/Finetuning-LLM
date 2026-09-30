from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
    .filter(F.col("avg_age") > 57)
    .select("segment", "city_values", "avg_age")
    .orderBy(F.col("avg_age").desc(), "segment")
)
