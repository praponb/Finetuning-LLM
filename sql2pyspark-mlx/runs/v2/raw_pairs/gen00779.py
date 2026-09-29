from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.store_id").isin([1, 2, 3, 6]))
    .groupBy(F.col("p.brand"))
    .agg(
        F.max(F.col("ss.units")).alias("max_units"),
        F.max(F.col("ss.sale_date")).alias("latest_sale_date"),
    )
)
