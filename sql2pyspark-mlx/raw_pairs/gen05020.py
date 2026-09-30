from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("category") == "Electronics")
    .groupBy("supplier_id")
    .agg(
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.max("launch_date").alias("last_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(
        F.col("s.city"),
        F.col("s.country"),
        F.col("st.stddev_price"),
        F.col("st.last_launch_date"),
    )
)
