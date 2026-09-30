from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.month(F.col("o.order_date")).isin([5, 7, 12]) | F.col("c.city").isin(["Tokyo", "Chiang Mai"]))
    .select(F.col("o.id"), F.col("o.shipping_fee"), F.col("c.country"), F.col("s.city"))
    .orderBy("id")
    .limit(50)
)
