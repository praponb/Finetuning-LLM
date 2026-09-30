from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipping_fee", "id")
w2 = Window.partitionBy("channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("discount", "id")
w5 = Window.partitionBy(F.month("order_date")).orderBy("is_web", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy(F.col("amount").desc_nulls_last(), "id")
w_part = Window.partitionBy("store_id").orderBy("running_shipping_fee", "id")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

cleaned = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-06-23").cast("date"))
    .withColumn("is_web", F.when(F.col("channel") == "web", 1).otherwise(0))
)

enriched = (
    cleaned
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w2))
    .withColumn("running_shipping_fee", F.sum("shipping_fee").over(w_recent))
    .withColumn("discount_bucket", F.ntile(10).over(w4))
    .withColumn("running_is_web", F.sum("is_web").over(w5))
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

metrics = (
    ranked
    .select(
        "id",
        "customer_id",
        "discount",
        "last_known_amount",
        "running_shipping_fee",
        "discount_bucket",
        "running_is_web",
        F.lead("running_shipping_fee").over(w_part).alias("next_running_shipping_fee"),
        F.lag("amount").over(w_part).alias("prev_amount"),
        F.lead("amount").over(w_part).alias("next_amount"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.discount",
        "s.last_known_amount",
        "s.running_shipping_fee",
        "s.prev_amount",
        "s.next_amount",
        F.col("c.status"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "discount",
        "last_known_amount",
        "running_shipping_fee",
        "next_amount",
    )
)
