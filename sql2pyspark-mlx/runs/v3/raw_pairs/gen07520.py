from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.countDistinct("price").alias("unique_brands"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("st.category_values"),
        F.col("st.unique_brands"),
    )
)
