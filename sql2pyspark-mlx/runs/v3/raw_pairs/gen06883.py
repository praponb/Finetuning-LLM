from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("brand").isin(["Initech", "Umbrella", "Acme"]))
    .groupBy("supplier_id")
    .agg(
        F.max("price").alias("max_price"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "left")
    .select(F.col("s.supplier_name"), F.col("S.max_price"), F.col("S.row_count"))
)
