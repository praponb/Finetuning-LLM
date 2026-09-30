from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("p.stock_qty"),
        F.col("p.price"),
        F.col("p.launch_date"),
        F.when(F.col("s.country") == "DE", 1).otherwise(0).alias("is_de"),
    )
    .orderBy(F.col("launch_date").desc())
)
