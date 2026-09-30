from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") == "Berlin")
    .groupBy(F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(F.round(F.avg(F.col("o.amount")), 2).alias("avg_amount"))
    .orderBy(F.col("opened_month").desc(), F.col("avg_amount").desc())
    .limit(20)
)
