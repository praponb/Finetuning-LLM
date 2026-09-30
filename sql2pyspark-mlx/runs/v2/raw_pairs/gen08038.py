from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") > 26)
    .select("id", "product_id", "sale_date")
    .unionByName(
        df_store_sales
        .filter(F.col("sale_date").between("2024-10-01", "2025-03-01"))
        .select("id", "product_id", "sale_date")
    )
    .distinct()
)
