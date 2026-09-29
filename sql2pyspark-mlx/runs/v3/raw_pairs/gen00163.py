from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-09-01", "2024-01-01"))
    .select("id", "sale_date")
    .orderBy(F.col("id").desc())
)
