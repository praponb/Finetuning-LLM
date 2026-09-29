from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("supplier_id").isin([8, 14, 35]))
    .groupBy("supplier_id")
    .agg(F.min("launch_date").alias("first_launch_date"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(F.col("s.rating"), F.col("st.first_launch_date"))
)
