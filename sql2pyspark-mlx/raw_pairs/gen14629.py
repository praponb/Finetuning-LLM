from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("t.cost_range"),
    )
)
