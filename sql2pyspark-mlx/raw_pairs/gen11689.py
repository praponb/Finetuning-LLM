from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("quantity").isNotNull()).alias("i"), F.col("p.id") == F.col("i.product_id"), "left_anti")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.price"),
    )
    .orderBy(F.col("category").desc())
)
