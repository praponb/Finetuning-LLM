from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.avg("price").alias("avg_price"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.avg_price") >= 480)
    .select(F.col("s.supplier_name"), F.col("s.rating"), F.col("ss.avg_price"))
)
