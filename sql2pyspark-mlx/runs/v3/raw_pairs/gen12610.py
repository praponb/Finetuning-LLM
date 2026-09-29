from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "right")
    .filter(F.col("ss.store_id") == 5)
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.store_id"),
        F.col("p.launch_date"),
        F.col("p.category"),
        F.col("s.rating"),
        F.col("s.country"),
    )
    .orderBy("id", F.col("store_id").desc())
)
