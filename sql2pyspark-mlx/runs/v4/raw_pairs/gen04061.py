from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("st.brand_values"),
        F.col("st.brand_list"),
    )
)
