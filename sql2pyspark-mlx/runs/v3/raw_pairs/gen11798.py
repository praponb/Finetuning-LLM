from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.product_id"),
        F.col("p.cost"),
        F.col("p.launch_date"),
        F.col("s.supplier_name"),
        F.col("s.country"),
    )
    .orderBy("supplier_name")
)
