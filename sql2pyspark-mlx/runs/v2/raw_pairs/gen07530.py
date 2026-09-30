from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
        F.col("p.product_name"),
        F.col("p.price"),
        F.col("s.city"),
    )
    .orderBy("price", F.col("sale_date").desc())
)
