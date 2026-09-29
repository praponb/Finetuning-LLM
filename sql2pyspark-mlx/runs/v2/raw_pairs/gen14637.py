from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 6, 8, 9]))
    .withColumn("sale_date_plus_months", F.add_months("sale_date", 3))
)
