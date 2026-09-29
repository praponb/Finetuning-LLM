from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("brand").isin(["Initech", "Globex", "Umbrella"]))
    .groupBy("supplier_id")
    .agg((F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .filter(F.col("st.stock_qty_range") >= 52)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.country"),
        F.col("st.stock_qty_range"),
    )
    .orderBy(F.col("rating").desc(), "country")
)
