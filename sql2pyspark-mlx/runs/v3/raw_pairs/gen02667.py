from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("stock_qty") < 219)
    .groupBy("supplier_id")
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0))) / F.count("*")), 1).alias("pct_globex"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("t.avg_price"),
        F.col("t.pct_globex"),
    )
    .orderBy(F.col("rating").desc())
)
