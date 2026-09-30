from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.countDistinct("rating").alias("unique_countrys"),
    )
    .filter(F.col("unique_countrys") > 5)
    .drop("unique_countrys")
    .orderBy(F.col("rating_range").desc())
)
