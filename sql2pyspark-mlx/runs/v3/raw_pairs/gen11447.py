from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0)).alias("sports_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.sports_count") > 27)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("t.min_launch_date"),
        F.col("t.sports_count"),
    )
    .orderBy(F.col("id").desc(), "supplier_name")
    .limit(10)
)
