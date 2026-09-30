from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("stock_qty") > 372)
    .groupBy("supplier_id")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.avg_cost") > 78)
    .select(F.col("suppliers.city"), F.col("t.avg_cost"))
)
