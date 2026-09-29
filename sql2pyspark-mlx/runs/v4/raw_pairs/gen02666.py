from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.array_contains("tags", "premium"))
    .groupBy("supplier_id")
    .agg(F.min("cost").alias("min_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .filter(F.col("st.min_cost") >= 650)
    .select(F.col("s.id"), F.col("s.rating"), F.col("st.min_cost"))
)
