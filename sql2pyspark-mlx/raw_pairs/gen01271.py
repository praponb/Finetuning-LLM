from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "sale"))
    .select("product_name", "launch_date")
    .intersect(
        df_products
        .filter(F.col("launch_date") < "2024-12-01")
        .select("product_name", "launch_date")
    )
    .orderBy("launch_date")
)
