from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("price")).otherwise(0)).alias("acme_price"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("supplier_totals"), F.col("suppliers.id") == F.col("supplier_totals.supplier_id"), "inner")
    .filter(F.col("supplier_totals.cost_range") >= 80)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.rating"),
        F.col("supplier_totals.cost_range"),
        F.col("supplier_totals.acme_price"),
    )
)
