from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .filter(~F.col("stores.region").isin(["Central", "North"]))
    .groupBy(
        F.when(F.col("stores.sqft") >= 14057, "high").when(F.col("stores.sqft") >= 6598, "mid").otherwise("low").alias("sqft_bucket"),
        F.col("stores.city"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("orders.channel"))).alias("channel_list"),
        F.countDistinct(F.col("orders.shipping_fee")).alias("unique_statuss"),
    )
    .filter(F.col("unique_statuss") >= 2)
    .orderBy("sqft_bucket", F.col("unique_statuss").desc(), "city")
    .limit(1)
)
