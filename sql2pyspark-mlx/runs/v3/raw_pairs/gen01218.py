from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Acme", F.col("price")).otherwise(0)).alias("acme_price"),
        F.sum(F.when(F.col("category") == "Electronics", 1).otherwise(0)).alias("electronics_count"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("supplier_totals"), F.col("suppliers.id") == F.col("supplier_totals.supplier_id"), "left")
    .filter(F.col("supplier_totals.acme_price") > 71)
    .select(
        F.col("suppliers.city"),
        F.col("suppliers.supplier_name"),
        F.col("supplier_totals.acme_price"),
        F.col("supplier_totals.electronics_count"),
    )
    .orderBy("acme_price")
)
