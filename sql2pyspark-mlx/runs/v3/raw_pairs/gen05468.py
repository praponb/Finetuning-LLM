from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("method").isin(["card", "promptpay", "paypal"]))
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
)

result = (
    df_orders.alias("orders")
    .join(payments_by_order.alias("payments_by_order"), F.col("orders.id") == F.col("payments_by_order.order_id"), "left")
    .select(
        F.col("orders.amount"),
        F.col("orders.shipping_fee"),
        F.col("payments_by_order.method_values"),
    )
)
