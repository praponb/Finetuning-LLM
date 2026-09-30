from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .filter(F.lower(F.trim(F.col("stores.store_name"))) == "old town")
    .groupBy(F.date_trunc("MONTH", F.col("stores.opened_date")).alias("opened_month"))
    .agg(F.sort_array(F.collect_set(F.col("orders.channel"))).alias("channel_list"))
    .orderBy(F.col("opened_month").desc())
)
