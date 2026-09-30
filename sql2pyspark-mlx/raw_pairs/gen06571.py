from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .groupBy(
        F.when(F.col("o.discount") >= 0.36, "high").when(F.col("o.discount") >= 0.17, "mid").otherwise("low").alias("discount_bucket"),
        F.col("o.status"),
    )
    .agg(
        F.sum(F.when(F.col("s.carrier") == "Kerry", F.col("s.cost")).otherwise(0)).alias("kerry_cost"),
    )
)
