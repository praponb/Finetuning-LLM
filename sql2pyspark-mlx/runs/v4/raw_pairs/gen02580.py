from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"), F.year(F.col("s.opened_date")).alias("opened_year"))
    .agg(F.round(F.avg(F.col("o.amount")), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") >= 79)
    .orderBy(F.col("avg_amount").desc(), "city", "opened_year")
    .limit(1)
)
