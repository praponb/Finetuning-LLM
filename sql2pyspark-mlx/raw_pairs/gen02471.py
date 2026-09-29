from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("store_id").isin([5, 6]))
    .groupBy("store_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .select(F.col("s.opened_date"), F.col("st.min_quantity"))
)
