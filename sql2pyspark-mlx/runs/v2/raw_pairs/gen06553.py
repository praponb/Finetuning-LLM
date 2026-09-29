from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("ss.store_id") == 2)
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.units"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("s.city"),
        F.col("s.rating"),
    )
)
