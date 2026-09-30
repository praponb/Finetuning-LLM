from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "outer")
    .filter((F.col("st.region") == "East") & (F.col("o.channel") != "partner"))
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("s.delivered_date"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("st.city"),
    )
    .orderBy(F.col("id").desc())
)
