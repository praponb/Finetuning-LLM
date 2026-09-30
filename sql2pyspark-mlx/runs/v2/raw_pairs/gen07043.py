from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") <= "2025-10-01")
    .select("category", "product_name")
    .unionByName(
        df_products
        .filter(F.col("product_name").like("%Run%"))
        .select("category", "product_name")
    )
)
