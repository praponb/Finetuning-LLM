from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.countDistinct("cost").alias("unique_categorys"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("st.unique_categorys"))
    .orderBy("id", F.col("unique_categorys").desc())
)
