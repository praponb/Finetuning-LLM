from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .filter(F.col("st.avg_cost") >= 310)
    .select(
        F.col("s.city"),
        F.col("s.country"),
        F.col("st.avg_cost"),
        F.col("st.category_values"),
    )
)
