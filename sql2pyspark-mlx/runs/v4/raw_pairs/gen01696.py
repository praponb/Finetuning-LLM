from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("st.last_launch_date"),
        F.col("st.total_stock_qty"),
    )
)
