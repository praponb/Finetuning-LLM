from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg((F.max("price") - F.min("price")).alias("price_range"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.price_range") >= 54)
    .select(F.col("suppliers.supplier_name"), F.col("t.price_range"))
    .orderBy("price_range", "supplier_name")
)
