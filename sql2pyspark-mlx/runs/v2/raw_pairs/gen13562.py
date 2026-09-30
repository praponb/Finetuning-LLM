from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") != "Tokyo")
    .groupBy(
        F.year(F.col("s.opened_date")).alias("opened_year"),
        F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"),
    )
    .agg(F.countDistinct(F.col("p.amount")).alias("unique_methods"))
    .orderBy("opened_month")
)
