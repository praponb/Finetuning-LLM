from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-02-01", "2025-03-07"))
    .select("id", "units", "store_id")
    .unionByName(
        df_store_sales
        .filter(F.col("product_id").isin([1, 3, 6, 7]))
        .select("id", "units", "store_id")
    )
)
