from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.size("tags") >= 1)
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0)).alias("grocery_count"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("supplier_summary"), F.col("suppliers.id") == F.col("supplier_summary.supplier_id"), "left")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
        F.col("supplier_summary.grocery_count"),
    )
    .orderBy(F.col("id").desc())
)
