from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count("price").alias("price_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "left")
    .select(F.col("s.country"), F.col("s.supplier_name"), F.col("S.price_count"))
)
