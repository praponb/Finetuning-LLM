from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(
        (F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.status")))).alias("status_values"),
    )
)
