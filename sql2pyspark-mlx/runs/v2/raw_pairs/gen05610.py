from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(1400, 1600) & (F.col("revenue") >= 1600) & F.col("revenue").isNull())
    .select("product_id")
    .orderBy(F.col("product_id").desc())
)
