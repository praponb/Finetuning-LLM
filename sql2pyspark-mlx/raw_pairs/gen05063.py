from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.supplier_name").isNull())
    .select(
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
        F.array_contains(F.col("p.tags"), "bestseller").alias("is_bestseller"),
    )
)
