from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.min("units").alias("min_units"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .filter(F.col("st.min_units") > 31)
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("st.min_units"),
    )
)
