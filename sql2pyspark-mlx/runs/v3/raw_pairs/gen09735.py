from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.product_id").isin([1, 4, 6, 9]))
    .select(F.col("ss.sale_date"), F.col("s.opened_date"), F.col("p.supplier_id"))
    .orderBy(F.col("sale_date").desc())
)
