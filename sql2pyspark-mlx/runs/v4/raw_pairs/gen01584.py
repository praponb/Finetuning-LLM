from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.count("rating").alias("rating_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
)
