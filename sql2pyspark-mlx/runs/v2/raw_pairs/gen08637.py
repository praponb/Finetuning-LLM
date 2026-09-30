from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") >= 20)
    .withColumn("revenue_rounded", F.round("revenue", 2))
    .withColumn("revenue_level", F.when(F.col("revenue") >= 3500, "High").when(F.col("revenue") >= 1100, "Medium").otherwise("Low"))
    .orderBy("revenue", "id")
    .limit(20)
)
