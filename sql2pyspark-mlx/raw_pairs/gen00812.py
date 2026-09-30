from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("launch_date").between("2024-12-25", "2025-06-10"))
    .groupBy("supplier_id")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(F.col("s.rating"), F.col("t.cost_range"))
    .orderBy(F.col("rating").desc(), "cost_range")
)
