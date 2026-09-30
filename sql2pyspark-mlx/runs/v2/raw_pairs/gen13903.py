from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter((F.col("s.opened_date") > "2023-03-01") & (F.col("s.city") == "London"))
    .select(F.col("s.store_name"), F.col("o.order_date"), F.col("o.amount"))
)
