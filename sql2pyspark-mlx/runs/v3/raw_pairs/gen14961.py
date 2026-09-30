from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .groupBy("supplier_id")
    .agg(F.max("launch_date").alias("last_launch_date"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("st.last_launch_date"),
    )
    .orderBy("last_launch_date")
)
