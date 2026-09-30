from pyspark.sql import functions as F

base = df_customers.filter((F.col("country") != "UK") & (F.length("name") >= 12))

agg = (
    base
    .groupBy("is_active")
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
)

result = (
    agg
    .orderBy("stddev_age", "is_active")
    .limit(5)
)
