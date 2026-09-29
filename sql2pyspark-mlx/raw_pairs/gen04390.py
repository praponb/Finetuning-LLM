from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.upper(F.col("s.store_name")).like("%OWN%"))
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"))
    .agg(F.min(F.col("p.amount")).alias("min_amount"))
    .orderBy("min_amount")
)
