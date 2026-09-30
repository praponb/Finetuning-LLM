from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.avg("stock_qty").alias("avg_stock_qty"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(F.col("s.city"), F.col("st.avg_stock_qty"))
    .orderBy(F.col("avg_stock_qty").desc())
)
