from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.min("cost").alias("min_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("st.min_cost"),
    )
    .orderBy("supplier_name", "id")
    .limit(10)
)
