from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("price") > 640)
    .groupBy("supplier_id")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.min("launch_date").alias("min_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(
        F.col("s.country"),
        F.col("s.rating"),
        F.col("st.category_list"),
        F.col("st.min_launch_date"),
    )
    .orderBy(F.col("rating").desc())
)
