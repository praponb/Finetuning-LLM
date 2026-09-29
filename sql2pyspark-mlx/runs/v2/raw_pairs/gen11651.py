from pyspark.sql import functions as F

store_totals = (
    df_orders
    .filter(F.col("amount") >= 920)
    .groupBy("store_id")
    .agg(
        F.sum(F.when(F.col("channel") == "store", 1).otherwise(0)).alias("store_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .filter(F.col("st.store_count") >= 31)
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.col("st.store_count"),
    )
    .orderBy(F.col("store_count").desc())
)
