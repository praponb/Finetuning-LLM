from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.revenue"),
        F.col("p.price"),
        F.col("s.country"),
        F.col("s.city"),
        F.coalesce(F.col("s.supplier_name"), F.lit("unassigned")).alias("supplier_name_filled"),
    )
    .orderBy("revenue", "city")
)
