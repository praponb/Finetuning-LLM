from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(~(F.array_contains(F.col("p.tags"), "bestseller") & (F.col("s.rating") < 2.4)))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.revenue"),
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("s.city"),
    )
    .orderBy("launch_date")
)
