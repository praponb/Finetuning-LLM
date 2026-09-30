from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.countDistinct("revenue").alias("unique_stores"))
    .filter(F.col("unique_stores") < 6)
    .select("product_id", "unique_stores")
    .orderBy(F.col("product_id").desc())
)
