from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 4]) | (F.col("store_id") == 4))
    .withColumn("revenue_per_unit", (F.col("revenue") + F.col("units")))
    .withColumn("sale_date_day", F.dayofmonth("sale_date"))
)
