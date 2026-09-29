from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0))) / F.count("*")), 1).alias("pct_initech"),
        F.round(F.avg("price"), 2).alias("avg_price"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.city"),
        F.col("t.pct_initech"),
        F.col("t.avg_price"),
    )
    .orderBy(F.col("pct_initech").desc())
)
