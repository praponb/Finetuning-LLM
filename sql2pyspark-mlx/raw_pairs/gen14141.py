from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "outer")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.month(F.col("orders.order_date")).isin([1, 6, 9]))
    .groupBy(
        F.col("orders.channel"),
        F.year(F.col("orders.order_date")).alias("order_year"),
    )
    .agg(F.sort_array(F.collect_set(F.col("shipments.carrier"))).alias("carrier_list"))
)
