from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(F.max("launch_date").alias("latest_launch_date"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("ss.latest_launch_date"),
    )
    .orderBy(F.col("id").desc())
    .limit(50)
)
