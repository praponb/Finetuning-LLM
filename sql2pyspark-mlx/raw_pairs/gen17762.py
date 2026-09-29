from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("amount", "id")
w2 = Window.partitionBy("status").orderBy("amount_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("order_date_period", "id")
w4 = Window.partitionBy("status").orderBy("order_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("customer_id").orderBy("unit_price", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("status").orderBy(F.col("order_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_part = Window.partitionBy("status", "channel").orderBy(F.col("order_item_quantity").desc(), "id")

valid = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("order_date") > F.lit("2023-07-24").cast("date")))
    .withColumn("amount_int", F.col("amount").cast("int"))
    .withColumn("order_date_period", F.date_trunc("week", "order_date"))
)

metrics = (
    valid
    .filter(F.col("status") == "COMPLETED")
    .select(
        "*",
        F.lag("discount").over(w).alias("prev_discount"),
        F.lag("shipping_fee").over(w).alias("prev_shipping_fee"),
        F.row_number().over(w).alias("event_seq"),
        F.last("discount", ignorenulls=True).over(w2).alias("last_known_discount"),
    )
)

with_prev = metrics.withColumn("prev_order_date_period", F.lag("order_date_period").over(w3))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_order_date_period").isNull() | (F.col("order_date_period") > F.col("prev_order_date_period") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_orders = (
    flagged
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "order_date_period",
        "prev_discount",
        "event_seq",
        "last_known_discount",
        F.sum("new_group_flag").over(w4).alias("session_id"),
    )
)

with_last = (
    sessions_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

with_windows = (
    with_last
    .select(
        "id",
        "status",
        "discount",
        "shipping_fee",
        "event_seq",
        F.avg("amount").over(w6).alias("amount_avg_5"),
        F.avg("amount").over(w7).alias("amount_avg_7d"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("shipping_fee") >= 38, "LARGE").otherwise("SMALL").alias("shipping_fee_tier"),
    )
    .agg(
        F.percentile_approx("shipping_fee", 0.9).alias("shipping_fee_p90"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
    )
    .orderBy(F.col("shipping_fee_p90").desc())
)
