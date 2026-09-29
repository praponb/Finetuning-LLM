from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.row_count") > 5)
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("st.row_count"),
    )
)
