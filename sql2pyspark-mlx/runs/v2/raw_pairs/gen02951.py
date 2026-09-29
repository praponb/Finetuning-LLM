from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 7, 8]))
    .groupBy("store_id")
    .agg(F.count("units").alias("units_count"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.units_count") >= 10)
    .select(F.col("s.store_name"), F.col("s.opened_date"), F.col("st.units_count"))
    .orderBy(F.col("opened_date").desc())
)
