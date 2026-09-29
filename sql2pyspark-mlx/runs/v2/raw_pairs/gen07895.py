from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .filter(F.col("p.supplier_id").isNotNull())
    .join(df_suppliers.filter(F.col("city").isin(["Tokyo", "Singapore"])).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_anti")
    .filter(F.array_contains(F.col("p.tags"), "clearance"))
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.category"),
        F.col("p.stock_qty"),
    )
)
