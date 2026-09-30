from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Fashion")
    .select("product_name", "launch_date")
    .unionByName(
        df_products
        .filter(F.col("launch_date").between("2024-09-28", "2025-02-01"))
        .select("product_name", "launch_date")
    )
)
