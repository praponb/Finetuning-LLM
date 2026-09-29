from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("units_tier", F.when(F.col("units") >= 31, "High").when(F.col("units") >= 15, "Medium").otherwise("Low"))
    .withColumn("sale_date_plus_67d", F.date_add("sale_date", 67))
)
