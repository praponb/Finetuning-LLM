from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("p.price"),
        F.col("p.brand"),
        F.col("s.city"),
        F.col("s.rating"),
    )
    .orderBy("rating")
)
