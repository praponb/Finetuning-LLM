from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.max("price").alias("max_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .filter(F.col("pbs.max_price") >= 410)
    .select(
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("pbs.last_launch_date"),
        F.col("pbs.max_price"),
    )
)
