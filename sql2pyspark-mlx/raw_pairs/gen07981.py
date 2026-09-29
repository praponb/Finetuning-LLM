from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") >= 12) & F.col("product_id").isin([6, 8, 9, 10]))
    .select("id", "sale_date", "product_id", "revenue")
    .orderBy(F.col("id").desc())
)
