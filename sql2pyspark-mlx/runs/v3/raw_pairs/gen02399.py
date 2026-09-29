from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.cost") <= 190)
    .select(
        F.col("s.country"),
        F.col("s.city"),
        F.col("p.stock_qty"),
        F.col("p.price"),
        F.col("p.launch_date"),
    )
)
