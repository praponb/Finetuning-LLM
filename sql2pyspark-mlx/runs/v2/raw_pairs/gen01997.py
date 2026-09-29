from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"))
    .agg(F.avg(F.col("o.amount")).alias("avg_amount"))
)
