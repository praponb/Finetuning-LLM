from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(18, 20))
    .withColumn("revenue_per_unit", F.round((F.col("revenue") / F.col("units")), 2))
    .withColumn("revenue_per_unit_2", (F.col("revenue") - F.col("units")))
    .orderBy(F.col("units").desc())
)
