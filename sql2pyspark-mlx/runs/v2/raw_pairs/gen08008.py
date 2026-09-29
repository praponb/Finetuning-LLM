from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") >= "2024-01-01")
    .select("category", "brand")
    .unionByName(
        df_products
        .filter(~F.col("category").isin(["Grocery", "Electronics"]))
        .select("category", "brand")
    )
    .distinct()
)
