from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .select(
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("p.product_name"),
        F.col("p.cost"),
        F.col("s.city"),
    )
    .orderBy(F.col("sale_date").desc())
)
