from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.sum("price").alias("total_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.total_price") > 5700)
    .select(
        F.col("s.country"),
        F.col("s.rating"),
        F.col("ss.last_launch_date"),
        F.col("ss.total_price"),
    )
    .orderBy(F.col("country").desc())
)
