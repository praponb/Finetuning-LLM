from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .filter(F.col("o.order_date") > "2024-12-21")
    .select(
        F.col("s.status"),
        F.col("s.order_id"),
        F.col("o.channel"),
        F.col("st.store_name"),
    )
    .orderBy(F.col("order_id").desc())
)
