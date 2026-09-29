from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(2.5, 2.7))
    .groupBy("country")
    .agg(F.countDistinct("rating").alias("unique_countrys"))
    .filter(F.col("unique_countrys") >= 9)
    .select("country", "unique_countrys")
    .orderBy("country")
)
