from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units").isNotNull() & (F.col("units") != 36)) | (F.col("units") < 32))
    .withColumn("revenue_int", F.col("revenue").cast("int"))
    .withColumn("units_filled", F.coalesce("units", F.lit(0)))
    .orderBy(F.col("units").desc(), "id")
    .limit(1)
)
