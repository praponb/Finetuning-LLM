from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("supplier_id").isin([4, 15, 36, 38]))
    .groupBy("supplier_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .filter(F.col("pbs.total_count") >= 6)
    .select(F.col("s.id"), F.col("s.city"), F.col("s.rating"), F.col("pbs.total_count"))
)
