from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "sale") & (F.length(F.col("p.product_name")) < 14))
    .select(
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("ss.store_id"),
        F.col("p.category"),
        F.col("p.cost"),
        F.col("p.supplier_id"),
    )
)
