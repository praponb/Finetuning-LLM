from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0)).alias("sports_count"),
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.grocery_count") > 8)
    .select(
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("t.sports_count"),
        F.col("t.grocery_count"),
    )
    .orderBy("sports_count")
)
