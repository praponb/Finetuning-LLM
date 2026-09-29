from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(F.max("last_restock").alias("last_last_restock"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .select(F.col("s.store_name"), F.col("st.last_last_restock"))
    .orderBy(F.col("store_name").desc())
)
