from pyspark.sql import functions as F

store_totals = (
    df_orders
    .filter(F.col("customer_id") == 35)
    .groupBy("store_id")
    .agg(
        F.countDistinct("shipping_fee").alias("unique_customers"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .filter(F.col("st.unique_customers") > 9)
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("st.unique_customers"),
        F.col("st.channel_list"),
    )
)
