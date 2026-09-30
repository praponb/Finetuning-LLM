from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("stock_qty").between(115, 215))
    .groupBy("supplier_id")
    .agg(F.sum("cost").alias("total_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(F.col("s.rating"), F.col("st.total_cost"))
    .orderBy(F.col("total_cost").desc(), "rating")
)
