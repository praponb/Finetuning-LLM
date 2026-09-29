from pyspark.sql import functions as F

store_totals = (
    df_orders
    .filter(F.col("amount") > 980)
    .groupBy("store_id")
    .agg(
        F.countDistinct("amount").alias("unique_statuss"),
        F.min("order_date").alias("first_order_date"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .select(
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.col("st.unique_statuss"),
        F.col("st.first_order_date"),
    )
)
