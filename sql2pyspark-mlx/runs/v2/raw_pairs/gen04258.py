from pyspark.sql import functions as F

store_totals = (
    df_orders
    .filter(F.col("customer_id") == 15)
    .groupBy("store_id")
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0)).alias("mobile_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .select(
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("st.web_count"),
        F.col("st.mobile_count"),
    )
    .orderBy(F.col("mobile_count").desc(), "region")
)
