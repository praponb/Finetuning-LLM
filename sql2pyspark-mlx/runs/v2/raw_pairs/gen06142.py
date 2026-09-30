from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .filter(F.col("orders.customer_id").isNotNull())
    .join(df_customers.filter(~F.col("is_active")).alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left_anti")
    .filter(F.col("orders.amount") < 990)
    .select(F.col("orders.id"), F.col("orders.order_date"))
)
