from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("stock_qty") > 207)
    .groupBy("supplier_id")
    .agg(F.avg("cost").alias("avg_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .filter(F.col("st.avg_cost") >= 660)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("st.avg_cost"),
    )
)
