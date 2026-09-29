from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .filter(F.col("units") < 21)
    .groupBy("store_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.sum("units").alias("total_units"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.total_units") > 200)
    .select(
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("st.avg_units"),
        F.col("st.total_units"),
    )
)
