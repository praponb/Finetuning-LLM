from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 6]))
    .groupBy("store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("st.max_quantity"),
        F.col("st.cnt"),
    )
    .orderBy(F.col("cnt").desc())
)
