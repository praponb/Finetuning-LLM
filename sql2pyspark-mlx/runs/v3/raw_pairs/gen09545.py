from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") >= "2025-05-08") & (F.col("sale_date") > "2025-05-01") & (F.year("sale_date") >= 2023))
    .select("id", "store_id")
    .orderBy(F.col("id").desc(), F.col("store_id").desc())
)
