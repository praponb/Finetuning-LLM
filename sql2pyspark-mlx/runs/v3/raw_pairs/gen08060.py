from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Lapto%"))
    .select("category", "brand")
    .unionByName(
        df_products
        .filter(F.col("supplier_id") == 27)
        .select("category", "brand")
    )
    .distinct()
)
