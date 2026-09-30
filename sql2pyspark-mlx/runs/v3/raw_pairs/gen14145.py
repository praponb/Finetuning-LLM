from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("order_id") == 34)
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("carrier") == "DHL", F.col("cost")).otherwise(0)).alias("dhl_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.dhl_cost") >= 29)
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
        F.col("ot.dhl_cost"),
    )
)
