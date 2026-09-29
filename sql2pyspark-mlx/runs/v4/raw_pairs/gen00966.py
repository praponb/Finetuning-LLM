from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.row_count") >= 3)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("t.row_count"),
    )
)
