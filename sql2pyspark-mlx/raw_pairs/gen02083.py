from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.array_contains("tags", "premium"))
    .groupBy("supplier_id")
    .agg(
        F.count("stock_qty").alias("stock_qty_count"),
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(
        F.col("s.rating"),
        F.col("s.country"),
        F.col("st.stock_qty_count"),
        F.col("st.stock_qty_range"),
    )
)
