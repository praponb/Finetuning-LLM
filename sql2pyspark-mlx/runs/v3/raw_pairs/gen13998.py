from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("brand") == "Stark")
    .groupBy("supplier_id")
    .agg(
        F.count("*").alias("row_count"),
        F.avg("price").alias("avg_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(F.col("s.country"), F.col("t.row_count"), F.col("t.avg_price"))
)
