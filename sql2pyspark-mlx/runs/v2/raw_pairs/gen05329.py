from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .select(
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.col("p.cost"),
        F.col("p.category"),
        F.col("s.rating"),
    )
    .orderBy("category", "sale_date")
)
