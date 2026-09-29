from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNotNull() & (F.col("units") <= 28))
    .withColumn("revenue_rounded", F.round("revenue", 0))
    .withColumn("revenue_per_unit", (F.col("units") - F.col("revenue")))
    .orderBy("revenue_per_unit")
)
