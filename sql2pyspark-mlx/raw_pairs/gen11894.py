from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("price").between(1040, 1200))
    .groupBy("supplier_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("st.brand_values"),
        F.col("st.brand_list"),
    )
    .orderBy(F.col("brand_values").desc())
)
