from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("stock_qty")).otherwise(0)).alias("electronics_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.avg_cost") >= 40)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("ss.avg_cost"),
        F.col("ss.electronics_stock_qty"),
    )
    .orderBy(F.col("id").desc())
)
