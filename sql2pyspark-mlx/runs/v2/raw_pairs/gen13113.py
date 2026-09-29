from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Fashion", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fashion"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.pct_fashion") >= 50)
    .select(F.col("s.city"), F.col("t.pct_fashion"))
    .orderBy(F.col("pct_fashion").desc(), "city")
)
