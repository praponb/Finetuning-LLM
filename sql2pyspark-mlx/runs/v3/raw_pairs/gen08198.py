from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("supplier_id").isin([13, 21, 30, 33]))
    .groupBy("supplier_id")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.cnt") > 9)
    .select(F.col("s.country"), F.col("ss.cnt"), F.col("ss.acme_count"))
)
