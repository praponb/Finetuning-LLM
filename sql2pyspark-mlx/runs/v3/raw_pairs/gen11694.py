from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 3000)
    .groupBy("store_id")
    .agg(
        F.countDistinct("units").alias("unique_products"),
        F.max("units").alias("max_units"),
    )
    .filter(F.col("unique_products") > 7)
    .orderBy(F.col("unique_products").desc(), "store_id")
    .limit(5)
)
