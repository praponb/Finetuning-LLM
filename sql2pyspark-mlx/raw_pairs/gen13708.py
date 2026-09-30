from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.shipping_fee") > 33)
    .groupBy(F.col("o.status"))
    .agg(F.count(F.when(F.col("s.carrier") == "DHL", 1)).alias("dhl_count"))
    .orderBy("dhl_count")
)
