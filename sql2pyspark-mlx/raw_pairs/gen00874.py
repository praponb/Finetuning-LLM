from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("o.status")))).alias("status_values"),
    )
    .orderBy("status_values")
)
