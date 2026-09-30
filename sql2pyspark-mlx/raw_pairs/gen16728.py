from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "customer_id").orderBy(F.col("amount").asc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
amount_by_store_id = df_orders.groupBy("store_id").agg(F.avg("amount").alias("avg_order_amount"))

kept = (
    df_orders
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") >= F.col("s.order_date")), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.quantity") < 8), "left_anti")
)

latest = (
    not_flagged
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_latest = (
    latest
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.order_date",
        "s.status",
        "s.row_num",
        F.col("c.paid_at").alias("payment_paid_at"),
        F.col("c.amount").alias("payment_amount"),
    )
)

kept_orders = (
    with_latest
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") <= F.col("s.order_date")), "left_anti")
)

kept_orders_2 = (
    kept_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.status") == "IN_TRANSIT"), "left_anti")
)

result = (
    kept_orders_2
    .join(amount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "row_num",
        "payment_amount",
        F.col("avg_order_amount"),
    )
    .orderBy(F.col("row_num").desc())
)
