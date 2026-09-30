from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id") == 1) & (F.col("revenue") < 2200) & F.col("revenue").between(1500, 2600))
    .select("units", "product_id", "revenue")
    .orderBy(F.col("revenue").desc())
)
