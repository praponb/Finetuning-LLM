from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0)).alias("stark_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(F.col("s.supplier_name"), F.col("s.country"), F.col("t.stark_count"))
)
