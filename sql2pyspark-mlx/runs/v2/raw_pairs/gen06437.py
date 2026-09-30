from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.credit_limit") < 13400)
    .groupBy(
        F.date_format(F.col("customers.signup_date"), "yyyy-MM").alias("signup_month"),
    )
    .agg(
        F.countDistinct(F.col("shipments.cost")).alias("unique_carriers"),
        F.min(F.col("shipments.cost")).alias("min_cost"),
    )
)
