from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("ss.store_id").isin([1, 2, 3]))
    .select(
        F.col("ss.product_id"),
        F.col("p.brand"),
        F.col("p.cost"),
        F.col("s.rating"),
    )
    .orderBy(F.col("brand").desc())
)
