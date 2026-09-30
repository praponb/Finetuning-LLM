from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") <= "2024-01-01")
    .select("id", "brand", "category")
    .unionByName(
        df_products
        .filter(F.array_contains("tags", "premium"))
        .select("id", "brand", "category")
    )
)
