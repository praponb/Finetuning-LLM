from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-05-18", "2024-11-01"))
    .select("units")
    .unionByName(
        df_store_sales
        .filter(F.col("product_id").isin([2, 3, 8]))
        .select("units")
    )
    .distinct()
)
