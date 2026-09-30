from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("launch_date") <= "2024-12-01")
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0)).alias("initech_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("pbs.initech_count"),
    )
    .orderBy(F.col("supplier_name").desc())
)
