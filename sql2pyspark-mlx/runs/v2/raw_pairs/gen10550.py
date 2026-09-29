from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.product_id").isin([2, 8, 10]))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("p.price"),
    )
)
