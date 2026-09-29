from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("t.avg_price"),
    )
    .orderBy("avg_price")
)
