from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-11-01", "2024-12-01"))
    .select("store_id", "sale_date")
)
