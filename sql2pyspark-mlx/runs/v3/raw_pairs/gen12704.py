from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"),
        F.sum(F.when(F.col("brand") == "Stark", F.col("cost")).otherwise(0)).alias("stark_cost"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("SupplierTotals"), F.col("suppliers.id") == F.col("SupplierTotals.supplier_id"), "left")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
        F.col("SupplierTotals.home_count"),
        F.col("SupplierTotals.stark_cost"),
    )
    .orderBy(F.col("home_count").desc(), F.col("id").desc())
)
