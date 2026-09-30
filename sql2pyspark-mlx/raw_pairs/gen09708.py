from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.store_id").isin([3, 4, 6]) & F.col("s.rating").between(2.7, 3.1))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("p.stock_qty"),
        F.col("s.supplier_name"),
        F.sort_array(F.col("p.tags")).alias("sorted_tags"),
    )
)
