from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("product_name").like("%mou%"))
    .groupBy("supplier_id")
    .agg(
        F.sum("price").alias("total_price"),
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("st.total_price"),
        F.col("st.category_values"),
    )
)
