from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.avg("age").alias("avg_age"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("avg_age") >= 43)
    .orderBy(F.col("avg_age").desc(), "country")
    .limit(50)
)
