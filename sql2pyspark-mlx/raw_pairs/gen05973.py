from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.store_id"),
        F.col("p.cost"),
        F.col("s.country"),
        (F.datediff(F.current_date(), F.col("ss.sale_date")) / 30).alias("months_since_sale_date"),
    )
    .orderBy(F.col("country").desc(), "id")
    .limit(1)
)
