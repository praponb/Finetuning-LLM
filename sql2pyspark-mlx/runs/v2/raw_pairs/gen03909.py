from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(~F.col("s.city").isin(["Bangkok", "New York"]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("p.cost"),
        F.col("p.category"),
        F.col("s.country"),
        F.col("s.city"),
    )
    .orderBy(F.col("store_id").desc(), "id")
    .limit(10)
)
