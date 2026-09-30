from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.count("units").alias("units_count"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .select(
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("st.units_count"),
        F.col("st.units_range"),
    )
    .orderBy(F.col("region").desc())
)
