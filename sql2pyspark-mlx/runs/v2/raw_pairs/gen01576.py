from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.max("stock_qty").alias("max_stock_qty"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("supplier_totals"), F.col("suppliers.id") == F.col("supplier_totals.supplier_id"), "inner")
    .filter(F.col("supplier_totals.max_stock_qty") > 170)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.rating"),
        F.col("suppliers.city"),
        F.col("supplier_totals.max_stock_qty"),
    )
    .orderBy("city", "id")
    .limit(20)
)
