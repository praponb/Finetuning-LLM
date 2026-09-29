from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.max("stock_qty").alias("max_stock_qty"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .filter(F.col("st.max_stock_qty") >= 390)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.country"),
        F.col("st.max_stock_qty"),
    )
    .orderBy(F.col("id").desc(), "rating")
    .limit(10)
)
