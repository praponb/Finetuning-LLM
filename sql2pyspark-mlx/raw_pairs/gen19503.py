from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", F.col("cost")).otherwise(0)).alias("initech_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.initech_cost") >= 29)
    .select(F.col("s.supplier_name"), F.col("ss.initech_cost"))
)
