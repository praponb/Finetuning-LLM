from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left_anti")
    .filter(F.col("p.launch_date").between("2023-03-01", "2025-05-01"))
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("p.category"),
    )
    .orderBy(F.col("id").desc())
)
