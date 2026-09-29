from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") >= 970)
    .select("id", "supplier_id", "product_name")
    .unionByName(
        df_products
        .filter(F.array_contains("tags", "eco"))
        .select("id", "supplier_id", "product_name")
    )
    .distinct()
)
