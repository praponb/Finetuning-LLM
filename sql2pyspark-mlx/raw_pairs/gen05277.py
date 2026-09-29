from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("s.city"),
    )
)
