from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.sum("price").alias("total_price"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(F.col("s.id"), F.col("s.rating"), F.col("ss.total_price"))
    .orderBy(F.col("total_price").desc(), "id")
    .limit(3)
)
