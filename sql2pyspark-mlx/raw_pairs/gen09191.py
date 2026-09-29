from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.count("cost").alias("cost_count"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0))) / F.count("*")), 1).alias("pct_stark"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .filter(F.col("st.pct_stark") > 78)
    .select(
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("st.cost_count"),
        F.col("st.pct_stark"),
    )
)
