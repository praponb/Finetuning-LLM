from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.launch_date").between("2024-05-01", "2025-01-03"))
    .select(
        F.col("s.country"),
        F.col("s.rating"),
        F.col("s.city"),
        F.col("p.price"),
        F.col("p.category"),
        F.col("p.stock_qty"),
    )
    .orderBy("category")
)
