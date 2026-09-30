from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter((F.col("p.price") > 940) & F.col("ss.store_id").isin([2, 4, 6]))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("p.category"),
        F.col("s.country"),
    )
    .orderBy(F.col("sale_date").desc(), F.col("product_id").desc())
)
