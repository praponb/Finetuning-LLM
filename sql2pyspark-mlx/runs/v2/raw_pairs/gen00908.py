from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.month("launch_date").isin([2, 5, 11]))
    .groupBy("supplier_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.min("launch_date").alias("min_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(F.col("s.city"), F.col("st.category_values"), F.col("st.min_launch_date"))
    .orderBy("min_launch_date", F.col("city").desc())
)
