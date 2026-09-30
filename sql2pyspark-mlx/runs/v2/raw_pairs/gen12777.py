from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.floor(F.col("o.shipping_fee")).alias("shipping_fee_whole"),
    )
    .orderBy(F.col("cost").desc(), "order_date", "id")
    .limit(20)
)
