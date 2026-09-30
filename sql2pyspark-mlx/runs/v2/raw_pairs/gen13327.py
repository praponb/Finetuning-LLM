from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("*").alias("total_count"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.avg_quantity") > 54)
    .select(
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("st.total_count"),
        F.col("st.avg_quantity"),
    )
    .orderBy("sqft", "total_count")
)
