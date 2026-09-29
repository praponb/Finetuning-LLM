from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("stock_qty") == 129)
    .groupBy("supplier_id")
    .agg(
        F.count("price").alias("price_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .filter(F.col("st.price_count") >= 4)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("st.price_count"),
        F.col("st.brand_values"),
    )
    .orderBy("brand_values")
)
