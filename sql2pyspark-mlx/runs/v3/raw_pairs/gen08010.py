from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 2.7)
    .groupBy(
        F.when(F.col("country").isin(["SG", "DE"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.min("rating").alias("min_rating"),
    )
    .filter(F.col("min_rating") > 1.9)
    .orderBy("country_group")
    .limit(5)
)
