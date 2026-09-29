from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.col("p.stock_qty") != 106) & (F.col("s.rating") > 4.2))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("p.cost"),
        F.col("s.rating"),
    )
)
