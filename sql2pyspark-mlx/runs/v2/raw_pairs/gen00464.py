from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .select(F.col("s.city"), F.col("s.country"), F.col("pbs.cost_range"))
)
