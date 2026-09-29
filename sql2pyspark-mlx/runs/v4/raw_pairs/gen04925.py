from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(((F.col("revenue") > 3200) & F.col("product_id").isin([1, 6, 7])) | (F.col("product_id") == 2))
    .orderBy(F.col("product_id").desc(), F.col("units").desc())
)
