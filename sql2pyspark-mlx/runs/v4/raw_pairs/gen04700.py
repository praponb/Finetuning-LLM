from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Siam%"))
    .groupBy("city")
    .agg(F.countDistinct("rating").alias("unique_citys"))
    .filter(F.col("unique_citys") >= 8)
    .select("city", "unique_citys")
    .orderBy("city")
)
