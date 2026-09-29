from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 3.8)
    .groupBy("country", "city")
    .agg(
        F.count("*").alias("total_count"),
        F.countDistinct("rating").alias("unique_countrys"),
    )
    .filter(F.col("unique_countrys") > 6)
)
