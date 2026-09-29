from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("launch_date") <= "2025-03-08")
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("price").alias("unique_categorys"),
        F.min("launch_date").alias("first_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .filter(F.col("st.unique_categorys") > 4)
    .select(
        F.col("s.rating"),
        F.col("st.unique_categorys"),
        F.col("st.first_launch_date"),
    )
    .orderBy("rating")
)
