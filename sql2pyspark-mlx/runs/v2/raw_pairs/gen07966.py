from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("p.cost") >= 220) & (F.col("ss.units") < 35))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("s.rating"),
    )
    .orderBy("store_id")
)
