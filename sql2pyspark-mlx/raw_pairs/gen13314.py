from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("order_date"), "status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")
w3 = Window.partitionBy(F.month("order_date"), "channel").orderBy(F.col("shipping_fee").desc(), "id")
w4 = Window.partitionBy(F.date_trunc("YEAR", "order_date"), "channel").orderBy("amount", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

filtered = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-09-26").cast("date"))
    .select("*", F.concat_ws("-", "status", "channel").alias("status_channel_key"))
)

metrics = (
    filtered
    .filter(F.col("channel") == "web")
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "discount",
        "shipping_fee",
        "status_channel_key",
        F.sum("discount").over(w).alias("cumulative_discount"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(10).over(w3).alias("shipping_fee_bucket"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

valid = (
    metrics
    .filter(F.col("shipping_fee") <= 34)
    .select("*", F.round((F.col("discount") * 1.15), 2).alias("adjusted_discount"))
)

latest_match = (
    valid
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.order_date",
        "s.amount",
        "s.discount",
        "s.status_channel_key",
        "s.cumulative_discount",
        "s.seq_no",
        "s.shipping_fee_bucket",
        "s.row_seq",
        F.col("c.unit_price"),
        F.col("c.quantity"),
    )
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.method").alias("payment_method"))
)

result = (
    with_latest
    .groupBy(F.date_trunc("year", "order_date").alias("year_start"))
    .agg(
        F.size(F.collect_set("payment_method")).alias("num_payment_methods"),
        F.size(F.collect_set("payment_method")).alias("num_payment_methods_2"),
        F.count(F.when(F.col("payment_method") == "promptpay", 1)).alias("promptpay_rows"),
    )
    .orderBy(F.col("promptpay_rows").desc())
)
