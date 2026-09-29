from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.size("tags") > 1)
    .groupBy("supplier_id")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.max("launch_date").alias("latest_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .filter(F.col("pbs.avg_cost") > 73)
    .select(F.col("s.rating"), F.col("pbs.avg_cost"), F.col("pbs.latest_launch_date"))
)
