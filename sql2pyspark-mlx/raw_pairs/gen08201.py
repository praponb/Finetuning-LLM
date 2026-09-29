from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.filter(F.col("amount") >= 530).alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left_semi")
    .filter(F.col("shipments.cost") >= 45)
    .select(
        F.col("shipments.id"),
        F.col("shipments.delivered_date"),
        F.col("shipments.carrier"),
    )
    .orderBy("carrier")
)
