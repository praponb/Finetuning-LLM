from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("p.cost") > 660) & F.col("p.supplier_id").isin([9, 16, 37, 39]))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("p.brand"),
        F.col("s.city"),
        F.col("s.country"),
    )
    .orderBy("country", "sale_date", "id")
    .limit(5)
)
