from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "left")
    .groupBy(
        F.when(F.col("st.sqft") >= 13919, "high").when(F.col("st.sqft") >= 2520, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.carrier")))).alias("carrier_values"),
        F.countDistinct(F.col("s.cost")).alias("unique_orders"),
    )
    .orderBy(F.col("carrier_values").desc())
)
