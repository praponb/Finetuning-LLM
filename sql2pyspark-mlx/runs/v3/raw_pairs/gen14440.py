from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.max("price").alias("max_price"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .select(F.col("s.id"), F.col("s.rating"), F.col("ss.max_price"))
    .orderBy(F.col("max_price").desc())
)
