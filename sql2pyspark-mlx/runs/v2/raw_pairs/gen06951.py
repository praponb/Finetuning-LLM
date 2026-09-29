from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("product_id").isin([7, 8]))
    .groupBy("store_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .filter(F.col("st.quantity_range") > 47)
    .select(F.col("s.id"), F.col("s.store_name"), F.col("st.quantity_range"))
    .orderBy(F.col("quantity_range").desc(), F.col("store_name").desc())
)
