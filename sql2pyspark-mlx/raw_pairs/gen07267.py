from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("price") > 770)
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Stark", F.col("price")).otherwise(0)).alias("stark_price"),
        F.min("launch_date").alias("first_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("ss.stark_price"),
        F.col("ss.first_launch_date"),
    )
    .orderBy("supplier_name", "id")
    .limit(5)
)
