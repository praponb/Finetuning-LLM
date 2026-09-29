from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2100, 3800) & (F.col("units") >= 31))
    .orderBy(F.col("product_id").desc(), "id")
    .limit(5)
)
