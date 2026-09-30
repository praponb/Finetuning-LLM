from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("order_id") == 24)
    .groupBy("order_id")
    .agg(
        F.count("*").alias("row_count"),
        F.min("delivered_date").alias("first_delivered_date"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.row_count") >= 7)
    .select(
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
        F.col("ot.row_count"),
        F.col("ot.first_delivered_date"),
    )
    .orderBy(F.col("customer_id").desc())
)
