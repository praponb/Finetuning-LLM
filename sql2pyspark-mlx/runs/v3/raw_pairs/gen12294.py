from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .groupBy(
        F.col("stores.region"),
        F.quarter(F.col("stores.opened_date")).alias("opened_quarter"),
    )
    .agg(
        (F.max(F.col("shipments.cost")) - F.min(F.col("shipments.cost"))).alias("cost_range"),
    )
)
