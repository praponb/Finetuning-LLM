from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("price").alias("min_price"),
        F.count("*").alias("num_products"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.num_products") >= 6)
    .select(F.col("s.city"), F.col("ss.min_price"), F.col("ss.num_products"))
    .orderBy(F.col("min_price").desc())
)
