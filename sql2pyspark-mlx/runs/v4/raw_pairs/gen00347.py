from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(F.countDistinct("rating").alias("unique_countrys"))
    .orderBy(F.col("unique_countrys").desc())
)
