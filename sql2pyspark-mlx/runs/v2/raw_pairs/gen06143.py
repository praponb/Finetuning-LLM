from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.avg("price").alias("avg_price"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(F.col("s.id"), F.col("s.country"), F.col("s.rating"), F.col("st.avg_price"))
    .orderBy(F.col("country").desc(), F.col("avg_price").desc())
)
