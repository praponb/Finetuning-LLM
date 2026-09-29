from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("category") == "Fashion")
    .groupBy("supplier_id")
    .agg(F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(F.col("s.id"), F.col("s.country"), F.col("st.stddev_stock_qty"))
)
