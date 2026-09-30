from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.sum("units").alias("total_units"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.total_units") >= 120)
    .select(F.col("s.id"), F.col("s.region"), F.col("s.sqft"), F.col("st.total_units"))
    .orderBy("region")
)
