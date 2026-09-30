from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent_2 = Window.partitionBy("status").orderBy("greatest_amount_shipping_fee", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("discount").alias("avg_order_discount"))

cleaned = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-06-17").cast("date"))
    .withColumn("greatest_amount_shipping_fee", F.greatest("amount", "shipping_fee"))
)

scored = (
    cleaned
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "discount",
        F.sum("greatest_amount_shipping_fee").over(w_recent).alias("running_sum_greatest_amount_shipping_fee"),
        F.min("greatest_amount_shipping_fee").over(w_recent).alias("running_min_greatest_amount_shipping_fee"),
        F.avg("greatest_amount_shipping_fee").over(w_recent).alias("running_avg_greatest_amount_shipping_fee"),
        F.max("greatest_amount_shipping_fee").over(w_recent).alias("running_max_greatest_amount_shipping_fee"),
        F.lag("greatest_amount_shipping_fee", 1, 0).over(w_recent_2).alias("prev_greatest_amount_shipping_fee"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"), F.col("c.method").alias("payment_method"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.unit_price") > 230), "left_anti")
)

not_flagged_orders = (
    not_flagged
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") < F.col("s.order_date")), "left_anti")
)

cleaned_orders = (
    not_flagged_orders
    .filter(F.col("prev_greatest_amount_shipping_fee").isNotNull())
    .withColumn("greatest_discount_running_min_greatest_amount_shipping_fee", F.greatest("discount", "running_min_greatest_amount_shipping_fee"))
)

result = (
    cleaned_orders
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "channel",
        "running_sum_greatest_amount_shipping_fee",
        "running_max_greatest_amount_shipping_fee",
        "payment_method",
        F.when(F.col("payment_method").rlike("^(card|paypal)$"), "GROUP_B").when(F.col("payment_method").rlike("^(bank_transfer|cash)$"), "OTHER_ZONE").otherwise("CORE").alias("payment_method_zone"),
        F.col("avg_order_discount"),
    )
    .orderBy("channel")
)
