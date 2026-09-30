from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.region"), F.col("st.stddev_units"))
    .orderBy("stddev_units", F.col("id").desc())
)
