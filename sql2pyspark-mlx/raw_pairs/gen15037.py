from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .filter(F.col("orders.discount") >= 0.16)
    .groupBy(
        F.when(F.col("stores.sqft") >= 16209, "high").when(F.col("stores.sqft") >= 2734, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.sort_array(F.collect_set(F.col("orders.channel"))).alias("channel_list"))
)
