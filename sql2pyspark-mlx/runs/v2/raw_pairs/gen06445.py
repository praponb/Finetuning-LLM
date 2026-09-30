from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.count("*").alias("num_products"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("cost")).otherwise(0)).alias("acme_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.num_products") >= 6)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.city"),
        F.col("ss.num_products"),
        F.col("ss.acme_cost"),
    )
    .orderBy("city")
)
