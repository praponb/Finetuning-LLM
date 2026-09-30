from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("revenue_per_unit", F.round((F.col("revenue") / F.col("units")), 2))
    .orderBy(F.col("units").desc(), "id")
    .limit(3)
)
