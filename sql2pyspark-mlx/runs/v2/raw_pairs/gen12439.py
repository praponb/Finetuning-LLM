from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("brand").isin(["Stark", "Globex", "Initech"]))
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.countDistinct("price").alias("unique_suppliers"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("ss.first_launch_date"),
        F.col("ss.unique_suppliers"),
    )
)
