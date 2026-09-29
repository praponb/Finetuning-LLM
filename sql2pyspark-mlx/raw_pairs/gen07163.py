from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~((F.year("sale_date") >= 2023) | (F.col("units") <= 39)))
    .withColumn("revenue_per_unit", (F.col("units") + F.col("revenue")))
    .withColumn("revenue_adjusted", F.round((F.col("revenue") * 1.1), 2))
    .orderBy(F.col("sale_date").desc(), "id")
    .limit(10)
)
