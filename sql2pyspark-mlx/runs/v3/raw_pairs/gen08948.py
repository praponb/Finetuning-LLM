from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.col("country") != "US").select("id")

result = (
    df_orders.alias("o")
    .join(excluded_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("o.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .filter(F.col("o.store_id").isin([11, 19]))
    .select(F.col("o.id"), F.col("o.order_date"), F.col("o.shipping_fee"))
    .orderBy(F.col("order_date").desc(), F.col("id").desc())
)
