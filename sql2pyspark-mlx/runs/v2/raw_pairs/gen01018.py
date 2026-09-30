from pyspark.sql import functions as F

result = (
    df_store_sales
    .agg(F.sum("units").alias("total_units"))
    .orderBy(F.col("total_units").desc())
)
