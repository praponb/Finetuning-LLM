from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .filter(F.col("pbs.umbrella_count") > 7)
    .select(F.col("s.id"), F.col("s.rating"), F.col("pbs.umbrella_count"))
    .orderBy("rating", "id")
    .limit(10)
)
