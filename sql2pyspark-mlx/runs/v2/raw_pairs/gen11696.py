from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .filter(F.col("p.supplier_id").isNotNull())
    .join(df_suppliers.filter(F.col("country") == "SG").alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_anti")
    .filter(F.array_contains(F.col("p.tags"), "sale"))
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
    )
)
