from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
        F.min("cost").alias("min_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(
        F.col("s.country"),
        F.col("s.rating"),
        F.col("t.home_count"),
        F.col("t.min_cost"),
    )
    .orderBy(F.col("min_cost").desc())
)
