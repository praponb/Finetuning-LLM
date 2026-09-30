from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.rating") <= 3.7)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.cost"),
        F.col("p.category"),
        F.col("s.city"),
        F.col("s.rating"),
        F.col("s.country"),
        F.col("p.stock_qty").cast("string").alias("stock_qty_str"),
    )
)
