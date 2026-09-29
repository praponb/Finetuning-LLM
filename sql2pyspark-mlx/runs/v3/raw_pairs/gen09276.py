from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.alias("orders"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.signup_date") <= "2023-10-14")
    .groupBy(F.col("customers.id"), F.col("customers.segment"))
    .agg(
        F.count(F.col("orders.id")).alias("order_count"),
        F.countDistinct(F.col("orders.discount")).alias("unique_stores"),
        F.min(F.col("orders.order_date")).alias("first_order_date"),
    )
)
