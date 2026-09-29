from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("s.store_name").like("%Cent%"))
    .groupBy(
        F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"),
        F.year(F.col("s.opened_date")).alias("opened_year"),
    )
    .agg(F.avg(F.col("o.amount")).alias("avg_amount"))
)
