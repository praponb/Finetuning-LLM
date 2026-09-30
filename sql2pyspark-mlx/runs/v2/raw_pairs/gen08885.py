from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.countDistinct("cost").alias("unique_brands"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("supplier_totals"), F.col("suppliers.id") == F.col("supplier_totals.supplier_id"), "left")
    .filter(F.col("supplier_totals.unique_brands") > 7)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("supplier_totals.unique_brands"),
    )
    .orderBy(F.col("unique_brands").desc())
)
