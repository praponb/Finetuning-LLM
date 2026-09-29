from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("stores.region") == "North")
    .groupBy(
        F.date_format(F.col("stores.opened_date"), "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.sum(F.when(F.col("orders.channel") == "web", F.col("orders.amount")).otherwise(0)).alias("web_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("orders.channel")))).alias("channel_values"),
    )
)
