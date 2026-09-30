from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .filter(F.col("o.customer_id") == 37)
    .groupBy(
        F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"),
        F.year(F.col("s.opened_date")).alias("opened_year"),
    )
    .agg(
        F.avg(F.col("p.amount")).alias("avg_amount"),
        F.count(F.col("p.amount")).alias("amount_count"),
    )
)
