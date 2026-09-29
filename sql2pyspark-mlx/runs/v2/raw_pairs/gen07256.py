from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("cost").between(600, 700))
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.avg("stock_qty").alias("avg_stock_qty"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("supplier_totals"), F.col("suppliers.id") == F.col("supplier_totals.supplier_id"), "left")
    .select(
        F.col("suppliers.city"),
        F.col("suppliers.country"),
        F.col("supplier_totals.min_launch_date"),
        F.col("supplier_totals.avg_stock_qty"),
    )
    .orderBy(F.col("avg_stock_qty").desc(), "city")
)
