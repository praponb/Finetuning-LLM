from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 1400)
    .agg(
        F.countDistinct("units").alias("unique_products"),
        F.sum("units").alias("total_units"),
    )
    .orderBy(F.col("total_units").desc())
)
