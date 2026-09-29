from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.count("units").alias("units_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("st.avg_revenue"),
        F.col("st.units_count"),
    )
)
