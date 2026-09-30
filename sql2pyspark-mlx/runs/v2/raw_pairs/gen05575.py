from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("stock_qty").between(124, 318))
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.sum(F.when(F.col("category") == "Fashion", 1).otherwise(0)).alias("fashion_count"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
        F.col("t.min_launch_date"),
        F.col("t.fashion_count"),
    )
    .orderBy(F.col("fashion_count").desc(), "id")
    .limit(10)
)
