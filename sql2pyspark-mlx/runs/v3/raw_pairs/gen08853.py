from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .groupBy(
        F.col("stores.region"),
        F.year(F.col("stores.opened_date")).alias("opened_year"),
    )
    .agg(
        F.round(F.avg(F.col("shipments.cost")), 2).alias("avg_cost"),
        (F.max(F.col("shipments.cost")) - F.min(F.col("shipments.cost"))).alias("cost_range"),
    )
    .orderBy(F.col("opened_year").desc(), F.col("avg_cost").desc())
)
