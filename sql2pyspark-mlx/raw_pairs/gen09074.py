from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("o.channel").isNotNull())
    .groupBy(
        F.col("s.city"),
        F.when(F.year(F.col("s.opened_date")) < 2024, "before_2024").when(F.year(F.col("s.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.status")))).alias("status_values"),
    )
)
