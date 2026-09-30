from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.product_name").isNull() & (F.col("s.rating") < 3.8))
    .select(
        F.col("s.country"),
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("p.stock_qty"),
    )
    .orderBy("launch_date")
)
