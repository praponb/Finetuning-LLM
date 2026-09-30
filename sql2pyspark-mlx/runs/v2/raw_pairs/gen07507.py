from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
        F.min("launch_date").alias("first_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.pct_acme") > 49)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("t.pct_acme"),
        F.col("t.first_launch_date"),
    )
    .orderBy("id")
)
