from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("supplier_id").isin([4, 16, 27, 33]))
    .groupBy("supplier_id")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("st.category_list"),
        F.col("st.home_count"),
    )
    .orderBy(F.col("id").desc(), F.col("home_count").desc())
)
