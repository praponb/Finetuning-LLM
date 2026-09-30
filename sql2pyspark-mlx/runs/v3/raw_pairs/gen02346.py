from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Electronics")
    .select("launch_date", "brand")
    .unionByName(df_products.filter(F.size("tags") >= 2).select("launch_date", "brand"))
    .distinct()
)
