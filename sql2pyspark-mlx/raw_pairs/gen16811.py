from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("payments.order_id").isNull())
    .groupBy(F.col("orders.channel"), F.col("orders.status"))
    .agg(
        F.sort_array(F.collect_set(F.col("payments.method"))).alias("method_list"),
        F.count(F.col("payments.amount")).alias("amount_count"),
    )
    .orderBy(F.col("amount_count").desc())
)
