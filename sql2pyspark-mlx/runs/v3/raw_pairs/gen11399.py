from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.max(F.col("p.amount")).alias("max_amount"),
        F.sort_array(F.collect_set(F.col("p.method"))).alias("method_list"),
    )
)
