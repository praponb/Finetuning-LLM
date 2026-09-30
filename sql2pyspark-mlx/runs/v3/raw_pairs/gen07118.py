from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("units_rounded", F.round("units", 1))
    .orderBy(F.col("revenue").desc(), "sale_date")
)
