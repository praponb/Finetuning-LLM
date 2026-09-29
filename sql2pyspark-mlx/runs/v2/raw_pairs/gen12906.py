from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.count("cost").alias("cost_count"),
        F.avg("price").alias("avg_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("S.cost_count"),
        F.col("S.avg_price"),
    )
    .orderBy(F.col("cost_count").desc(), "supplier_name", "id")
    .limit(5)
)
