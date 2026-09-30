from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count("*").alias("product_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.product_count") > 5)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.city"),
        F.col("ss.product_count"),
    )
    .orderBy("product_count")
)
