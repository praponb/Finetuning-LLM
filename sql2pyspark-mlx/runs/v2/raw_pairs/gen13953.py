from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "right")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .filter(F.year(F.col("stores.opened_date")) == 2025)
    .groupBy(F.col("orders.channel"))
    .agg(
        F.sum(F.when(F.col("shipments.carrier") == "Kerry", F.col("shipments.cost")).otherwise(0)).alias("kerry_cost"),
        (F.max(F.col("shipments.cost")) - F.min(F.col("shipments.cost"))).alias("cost_range"),
    )
    .orderBy("channel", F.col("kerry_cost").desc())
    .limit(50)
)
