from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("launch_date") <= "2024-05-01")
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0))) / F.count("*")), 1).alias("pct_stark"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("pbs.pct_stark"),
        F.col("pbs.avg_cost"),
    )
    .orderBy(F.col("pct_stark").desc(), F.col("id").desc())
)
