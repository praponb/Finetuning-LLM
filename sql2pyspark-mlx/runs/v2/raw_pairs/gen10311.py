from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "right")
    .filter(F.col("st.city").isin(["New York", "Singapore"]))
    .groupBy(
        F.col("st.city"),
        F.quarter(F.col("st.opened_date")).alias("opened_quarter"),
    )
    .agg(
        F.sum(F.when(F.col("s.carrier") == "Kerry", F.col("s.cost")).otherwise(0)).alias("kerry_cost"),
        F.max(F.col("s.delivered_date")).alias("latest_delivered_date"),
    )
)
