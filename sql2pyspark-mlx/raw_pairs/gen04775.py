from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"),
        F.col("s.region"),
    )
    .agg(F.sort_array(F.collect_set(F.col("o.channel"))).alias("channel_list"))
)
