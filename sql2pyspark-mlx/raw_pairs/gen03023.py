from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.category").isin(["Sports", "Books", "Grocery"]) & (F.col("p.launch_date") <= "2023-10-01"))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.revenue"),
        F.col("p.price"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
    )
)
