from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("category") != "Electronics")
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sports"),
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("SupplierSummary"), F.col("suppliers.id") == F.col("SupplierSummary.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.city"),
        F.col("suppliers.country"),
        F.col("SupplierSummary.pct_sports"),
        F.col("SupplierSummary.stock_qty_range"),
    )
    .orderBy(F.col("stock_qty_range").desc())
)
