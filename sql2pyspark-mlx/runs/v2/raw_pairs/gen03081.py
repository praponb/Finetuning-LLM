from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([5, 10]) & (F.col("revenue") >= 1700))
    .withColumn("revenue_per_unit", (F.col("units") + F.col("revenue")))
    .withColumn("sale_date_label", F.date_format("sale_date", "yyyy-MM-dd"))
)
