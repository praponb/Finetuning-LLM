from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") < 22)
    .groupBy("product_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.max("units").alias("max_units"),
        F.count("units").alias("units_count"),
    )
    .filter(F.col("max_units") > 15)
    .orderBy(F.col("max_units").desc())
)
