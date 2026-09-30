from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .filter(F.col("units") <= 15)
    .groupBy("store_id")
    .agg(F.min("revenue").alias("min_revenue"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .filter(F.col("st.min_revenue") > 1800)
    .select(F.col("s.store_name"), F.col("s.city"), F.col("st.min_revenue"))
    .orderBy(F.col("store_name").desc())
)
