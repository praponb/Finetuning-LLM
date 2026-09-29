from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(F.min("launch_date").alias("min_launch_date"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .select(F.col("s.rating"), F.col("s.country"), F.col("pbs.min_launch_date"))
    .orderBy("min_launch_date")
)
