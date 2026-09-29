from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "left")
    .filter(F.col("st.opened_date") > "2024-08-08")
    .select(
        F.col("s.shipped_date"),
        F.col("s.carrier"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.col("st.opened_date"),
        F.col("st.city"),
    )
    .orderBy("city")
)
