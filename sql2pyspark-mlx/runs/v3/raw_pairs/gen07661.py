from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("last_restock").between("2023-04-01", "2023-08-20"))
    .groupBy("store_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.region"), F.col("s.city"), F.col("st.min_quantity"))
    .orderBy(F.col("id").desc())
)
