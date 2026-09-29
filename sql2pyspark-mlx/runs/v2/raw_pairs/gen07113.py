from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.count("*").alias("num_store_sales"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .select(F.col("s.city"), F.col("st.max_revenue"), F.col("st.num_store_sales"))
)
