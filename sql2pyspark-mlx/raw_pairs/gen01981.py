from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") <= "2025-03-10")
    .select("product_name", "launch_date")
    .unionByName(
        df_products
        .filter(F.col("cost") >= 610)
        .select("product_name", "launch_date")
    )
    .distinct()
)
