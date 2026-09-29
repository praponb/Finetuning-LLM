from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3, 7, 10]))
    .withColumn("units_whole", F.ceil("units"))
    .withColumn("sale_date_plus_months", F.add_months("sale_date", 6))
)
