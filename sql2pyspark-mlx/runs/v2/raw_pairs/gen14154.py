from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") < 2300) & F.col("product_id").isin([1, 8, 9, 10]) & (F.col("sale_date") >= "2025-07-10"))
    .select("id", "store_id")
    .orderBy(F.col("store_id").desc())
)
