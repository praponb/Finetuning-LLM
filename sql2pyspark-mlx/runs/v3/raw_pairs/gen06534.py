from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("stock_qty") != 160)
    .groupBy("supplier_id")
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.count("*").alias("num_products"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .select(F.col("s.city"), F.col("pbs.latest_launch_date"), F.col("pbs.num_products"))
)
