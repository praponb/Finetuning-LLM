from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.month(F.col("s.delivered_date")).isin([5, 8, 12]) & F.col("s.carrier").isin(["FedEx", "Flash", "DHL"]))
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.order_id"),
        F.col("o.customer_id"),
        F.col("st.sqft"),
    )
)
