from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter((F.col("p.category") == "Grocery") & F.col("p.stock_qty").between(238, 305))
    .select(F.col("i.product_id"), F.col("i.store_id"), F.col("p.product_name"))
    .orderBy(F.col("product_name").desc())
)
