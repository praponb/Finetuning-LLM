from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.sum("cost").alias("total_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.total_cost") > 2700)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("t.first_launch_date"),
        F.col("t.total_cost"),
    )
    .orderBy(F.col("total_cost").desc())
)
