from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNotNull() | (F.col("revenue") > 1500))
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
    .orderBy(F.col("avg_units").desc())
)
