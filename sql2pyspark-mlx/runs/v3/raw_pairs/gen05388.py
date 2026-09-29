from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.countDistinct("rating").alias("unique_countrys"),
    )
    .orderBy(F.col("avg_rating").desc())
)
