from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0))) / F.count("*")), 1).alias("pct_globex"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("pbs.pct_globex"))
    .orderBy(F.col("id").desc())
)
