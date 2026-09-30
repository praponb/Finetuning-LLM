from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(F.countDistinct("rating").alias("unique_countrys"))
    .filter(F.col("unique_countrys") < 6)
    .select("country", "unique_countrys")
    .orderBy("country")
)
