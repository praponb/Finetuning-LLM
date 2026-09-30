from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2025-07-20", "2025-09-01"))
    .select("id", "units", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("store_id") == 3)
        .select("id", "units", "product_id")
    )
)
