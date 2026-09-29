from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.quantity").between(147, 149) & F.col("i.product_id").isin([3, 5, 9]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("p.brand"),
        F.col("p.product_name"),
    )
    .orderBy(F.col("id").desc())
)
