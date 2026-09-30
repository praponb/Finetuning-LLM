from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter((F.col("s.cost") <= 57) & (F.col("s.cost") >= 76))
    .select(
        F.col("s.carrier"),
        F.col("o.status"),
        F.col("o.channel"),
        F.col("st.opened_date"),
    )
    .orderBy(F.col("channel").desc())
)
