from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.count("*").alias("num_products"),
        F.max("launch_date").alias("latest_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .filter(F.col("pbs.num_products") >= 3)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.city"),
        F.col("pbs.num_products"),
        F.col("pbs.latest_launch_date"),
    )
    .orderBy(F.col("id").desc())
)
