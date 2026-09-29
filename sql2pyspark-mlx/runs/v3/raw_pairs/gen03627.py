from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .filter(F.col("s.country").isin(["SG", "JP"]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("p.cost"),
        F.col("s.rating"),
        F.col("s.city"),
    )
    .orderBy("rating")
)
