from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count("cost").alias("cost_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .filter(F.col("st.cost_count") >= 8)
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("st.cost_count"))
    .orderBy("id")
)
