from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("p.stock_qty"),
        F.col("s.country"),
    )
    .orderBy(F.col("stock_qty").desc())
)
