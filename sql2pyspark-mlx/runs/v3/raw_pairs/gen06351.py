from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2025-02-01", "2025-11-01"))
    .select("product_id", "sale_date")
    .unionByName(
        df_store_sales
        .filter(F.col("revenue") <= 1800)
        .select("product_id", "sale_date")
    )
)
