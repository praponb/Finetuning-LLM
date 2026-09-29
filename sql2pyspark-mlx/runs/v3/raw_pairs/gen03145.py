from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.upper("product_name").like("%MAR%"))
    .groupBy("supplier_id")
    .agg(F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .select(F.col("s.country"), F.col("ss.globex_count"))
    .orderBy(F.col("globex_count").desc())
)
