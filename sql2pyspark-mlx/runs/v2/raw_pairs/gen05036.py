from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units").isNotNull() & (F.col("product_id") == 1)) | F.col("store_id").isin([1, 4, 5, 6]))
)
