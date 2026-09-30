from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("city").isin(["New York", "Singapore"]), "priority").otherwise("other").alias("city_group"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.countDistinct("rating").alias("unique_countrys"),
    )
)
