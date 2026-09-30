from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(~F.col("carrier").isin(["Flash", "FedEx", "Kerry"])).alias("s"), F.col("o.id") == F.col("s.order_id"), "left_anti")
    .select(F.col("o.id"), F.col("o.shipping_fee"), F.col("o.order_date"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
