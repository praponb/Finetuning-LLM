from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("p.stock_qty"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.round(F.col("s.rating"), 1).alias("rating_rounded"),
    )
    .orderBy(F.col("stock_qty").desc(), "country")
)
