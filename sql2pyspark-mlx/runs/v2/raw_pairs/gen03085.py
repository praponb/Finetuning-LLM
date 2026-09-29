from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-02-01", "2024-11-03"))
    .select("units", "sale_date")
    .unionByName(
        df_store_sales
        .filter(F.col("sale_date").between("2024-10-01", "2025-03-17"))
        .select("units", "sale_date")
    )
    .distinct()
)
