from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.countDistinct("rating").alias("unique_countrys"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
    )
    .orderBy("country")
)
