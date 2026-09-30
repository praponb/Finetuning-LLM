from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 3.3)
    .groupBy("city")
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.countDistinct("rating").alias("unique_citys"),
    )
)
