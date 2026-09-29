from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.count("units").alias("units_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .filter(F.col("st.stddev_units") >= 78)
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("st.stddev_units"),
        F.col("st.units_count"),
    )
    .orderBy("region")
)
