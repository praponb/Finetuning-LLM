from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"),
        F.col("s.city"),
    )
    .agg(
        F.countDistinct(F.col("p.amount")).alias("unique_methods"),
        F.max(F.col("p.amount")).alias("max_amount"),
    )
    .orderBy(F.col("max_amount").desc(), "city")
)
