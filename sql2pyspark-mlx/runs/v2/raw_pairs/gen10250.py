from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.year("launch_date") == 2025)
    .select("id", "brand", "product_name")
    .intersect(
        df_products
        .filter(F.size("tags") > 1)
        .select("id", "brand", "product_name")
    )
    .orderBy("id")
)
