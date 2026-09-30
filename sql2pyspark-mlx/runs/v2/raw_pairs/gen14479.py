from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("ss.product_id").isin([1, 3, 5, 8]))
    .groupBy(F.col("s.country"))
    .agg(F.min(F.col("ss.sale_date")).alias("min_sale_date"))
    .orderBy(F.col("min_sale_date").desc())
)
