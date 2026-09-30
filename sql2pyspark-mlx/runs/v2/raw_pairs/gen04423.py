from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.upper("product_name").like("%FFI%"))
    .select("supplier_id", "category")
    .unionByName(
        df_products
        .filter(F.col("launch_date").between("2023-04-01", "2024-05-01"))
        .select("supplier_id", "category")
    )
)
