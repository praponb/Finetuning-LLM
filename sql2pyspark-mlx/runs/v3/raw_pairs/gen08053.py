from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(~F.col("brand").isin(["Acme", "Stark", "Initech"]))
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.total_count") >= 7)
    .select(
        F.col("s.supplier_name"),
        F.col("ss.first_launch_date"),
        F.col("ss.total_count"),
    )
    .orderBy(F.col("total_count").desc())
)
