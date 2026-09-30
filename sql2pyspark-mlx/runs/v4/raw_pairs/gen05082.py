from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("amount", "id")
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("shipping_fee").alias("sum_order_shipping_fee"))

eligible = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.product_id").isNull()), "left_anti")
)

scored = (
    eligible
    .select(
        "*",
        F.last("channel", ignorenulls=True).over(w).alias("last_known_channel"),
        F.last("amount", ignorenulls=True).over(w_hist).alias("last_known_amount"),
        F.ntile(10).over(w_recent).alias("amount_bucket"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") >= F.col("s.order_date")), "left_anti")
)

wide = (
    not_flagged
    .groupBy("customer_id")
    .pivot("status", ["SHIPPED", "COMPLETED", "RETURNED"])
    .agg(F.max("last_known_amount").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)

result = (
    wide
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "SHIPPED_value_sum",
        "COMPLETED_value_sum",
        "RETURNED_value_sum",
        "RETURNED_n",
        F.col("sum_order_shipping_fee"),
    )
    .orderBy(F.col("RETURNED_value_sum").asc_nulls_last(), "SHIPPED_value_sum")
)
