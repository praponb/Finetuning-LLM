from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("brand").isin(["Globex", "Stark"]))
    .groupBy("supplier_id")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.cost_range") > 76)
    .select(F.col("s.id"), F.col("s.rating"), F.col("t.cost_range"))
    .orderBy(F.col("cost_range").desc())
)
