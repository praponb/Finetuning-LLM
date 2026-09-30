from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.max("price").alias("max_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.max_price") > 660)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("ss.avg_cost"),
        F.col("ss.max_price"),
    )
)
