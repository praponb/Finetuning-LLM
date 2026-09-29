from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("amount").desc())
w3 = Window.partitionBy("store_id", "channel").orderBy("discount", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("customer_id", "status").orderBy("order_date", "id")
w6 = Window.orderBy("discount", "id")
w7 = Window.partitionBy("status").orderBy(F.col("shipping_fee_bucket").desc(), "id")
w8 = Window.orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("web"))) & (F.col("order_date") >= F.to_date(F.lit("2023-04-25"))))
    .withColumn("years_since_order_date", (F.datediff(F.current_date(), "order_date") / 365.25))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("UNKNOWN")))
)

enriched = (
    cleaned
    .filter(F.col("customer_id") == 34)
    .select(
        "*",
        F.ntile(10).over(w).alias("shipping_fee_bucket"),
        F.rank().over(w2).alias("amount_rank"),
        F.avg("shipping_fee").over(w3).alias("shipping_fee_avg_3_prior"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.order_date",
        "s.status",
        "s.amount",
        "s.discount",
        "s.years_since_order_date",
        "s.resolved_channel",
        "s.shipping_fee_bucket",
        "s.amount_rank",
        F.col("c.unit_price"),
        F.col("c.quantity"),
    )
)

deduped = (
    latest_match
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "order_date",
        "status",
        "discount",
        "resolved_channel",
        "quantity",
        F.ntile(3).over(w6).alias("discount_bucket"),
        F.ntile(5).over(w7).alias("shipping_fee_bucket_bucket"),
        F.avg("years_since_order_date").over(w8).alias("years_since_order_date_avg_6"),
    )
)

filtered = (
    with_windows
    .filter(F.col("years_since_order_date_avg_6").between(26, 75))
    .select(
        "id",
        "status",
        "discount",
        "resolved_channel",
        "quantity",
        "shipping_fee_bucket_bucket",
        F.least("discount", "discount_bucket").alias("least_discount_discount_bucket"),
    )
)

cleaned_orders = (
    filtered
    .filter(F.col("status").isin(["SHIPPED", "CANCELLED"]))
    .withColumn("shipping_fee_bucket_bucket_minus_quantity", (F.col("shipping_fee_bucket_bucket") - F.col("quantity")))
)

result = (
    cleaned_orders
    .select("id", "resolved_channel", "shipping_fee_bucket_bucket")
    .orderBy(F.col("id").desc_nulls_first())
    .limit(5)
)
