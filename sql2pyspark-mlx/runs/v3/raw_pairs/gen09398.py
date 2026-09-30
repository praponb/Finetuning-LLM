from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3]))
    .select("id", "sale_date", "product_id")
    .unionByName(
        df_store_sales
        .filter(F.col("sale_date").between("2025-04-01", "2025-06-29"))
        .select("id", "sale_date", "product_id")
    )
)
