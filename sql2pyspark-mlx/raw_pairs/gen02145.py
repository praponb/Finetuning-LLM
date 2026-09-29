from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("product_name").like("%wireles%"))
    .groupBy("supplier_id")
    .agg(F.max("launch_date").alias("latest_launch_date"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("st.latest_launch_date"),
    )
    .orderBy("latest_launch_date", F.col("city").desc())
)
