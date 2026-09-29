from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("s.region"),
        F.col("p.supplier_id"),
        F.col("p.price"),
        F.datediff(F.current_date(), F.col("ss.sale_date")).alias("days_since_sale_date"),
    )
    .orderBy("sale_date", "id")
    .limit(20)
)
