from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter(F.col("p.product_name").isNotNull())
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
    .orderBy("stock_qty")
)
