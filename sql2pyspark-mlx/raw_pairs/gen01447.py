from pyspark.sql import functions as F

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-07-20").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("mobile"))))
    .withColumn("amount_with_shipping", (F.col("shipping_fee") - F.col("amount")))
    .withColumn("shipping_fee_times_discount", (F.col("shipping_fee") * F.col("discount")))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("UNKNOWN")))
)

result = (
    valid
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") >= F.col("s.order_date")), "left_semi")
)
