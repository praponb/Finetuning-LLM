from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("s.sqft") > 7943)
    .groupBy(F.year(F.col("s.opened_date")).alias("opened_year"))
    .agg(F.count(F.col("o.amount")).alias("amount_count"))
)
