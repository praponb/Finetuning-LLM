from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "right")
    .filter(F.col("ss.units").between(19, 35))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.col("p.supplier_id"),
        F.col("s.supplier_name"),
    )
    .orderBy(F.col("sale_date").desc())
)
