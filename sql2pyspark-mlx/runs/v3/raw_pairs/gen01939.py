from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.rating") < 2.6)
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.units"),
        F.col("p.product_name"),
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.concat(F.col("s.supplier_name"), F.lit(" - "), F.col("s.city")).alias("supplier_name_city"),
    )
    .orderBy("city")
)
