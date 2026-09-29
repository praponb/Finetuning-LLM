from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .groupBy(
        F.col("st.city"),
        F.date_trunc("MONTH", F.col("st.opened_date")).alias("opened_month"),
    )
    .agg(F.countDistinct(F.col("s.cost")).alias("unique_carriers"))
    .filter(F.col("unique_carriers") > 5)
    .orderBy("opened_month", "city")
    .limit(5)
)
