from pyspark.sql import functions as F

result = (
    df_products
    .select("cost")
    .union(
        df_store_sales
        .filter(F.col("sale_date").between("2023-06-01", "2024-03-01"))
        .select("revenue")
    )
    .orderBy(F.col("cost").desc())
)
