from pyspark.sql import functions as F

store_totals = (
    df_orders
    .filter(~F.col("channel").isin(["mobile", "store", "partner"]))
    .groupBy("store_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .filter(F.col("st.avg_amount") >= 27)
    .select(F.col("s.sqft"), F.col("st.avg_amount"))
)
