from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("price") > 710)
    .groupBy("supplier_id")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.avg("stock_qty").alias("avg_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.city"),
        F.col("st.total_stock_qty"),
        F.col("st.avg_stock_qty"),
    )
    .orderBy("avg_stock_qty")
)
