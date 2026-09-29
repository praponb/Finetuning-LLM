from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") > "2023-04-01") & (F.col("sale_date") <= "2025-02-01"))
    .select("product_id", "store_id")
    .orderBy(F.col("store_id").desc())
)
