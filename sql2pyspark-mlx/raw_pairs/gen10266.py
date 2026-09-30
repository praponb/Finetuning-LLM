from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_filled", "id")
w2 = Window.partitionBy("status", "store_id").orderBy(F.col("shipping_fee").desc(), "id")
w3 = Window.partitionBy("channel").orderBy("shipping_fee", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.orderBy("amount", "id")
w5 = Window.partitionBy("channel").orderBy("shipping_fee", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy("shipping_fee", "id")
w_part = Window.partitionBy(F.month("order_date"), "channel").orderBy(F.col("order_date").desc(), "id")
w8 = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("channel").orderBy(F.col("discount_avg_5").desc(), "id")
w_part_2 = Window.partitionBy("channel", "store_id").orderBy("discount_avg_5", "id").rowsBetween(-3, Window.currentRow)
w11 = Window.partitionBy("store_id").orderBy("amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("customer_id").orderBy("prev_discount", "id")
w13 = Window.partitionBy("channel", "status").orderBy(F.col("discount_avg_3").desc())

base = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-08-19").cast("date"))
    .select(
        "*",
        F.floor("shipping_fee").alias("shipping_fee_whole"),
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.lag("shipping_fee", 2).over(w).alias("prev_shipping_fee"),
        F.row_number().over(w2).alias("event_seq"),
        F.avg("discount").over(w3).alias("discount_avg_5"),
        F.ntile(5).over(w4).alias("amount_bucket"),
        F.avg("discount").over(w5).alias("discount_avg_3"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.lag("discount", 2, 0).over(w6).alias("prev_discount"),
        F.row_number().over(w_part).alias("seq_no"),
        F.sum("discount_filled").over(w8).alias("cumulative_discount_filled"),
        F.row_number().over(w_running).alias("row_seq"),
        F.avg("discount_avg_3").over(w_part_2).alias("discount_avg_3_avg_3"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "shipping_fee_whole",
        "prev_shipping_fee",
        "discount_avg_5",
        "amount_bucket",
        "discount_avg_3",
        "seq_no",
        "row_seq",
        "discount_avg_3_avg_3",
        F.last("discount_avg_3_avg_3", ignorenulls=True).over(w11).alias("last_known_discount_avg_3_avg_3"),
        F.lag("seq_no", 1, 0).over(w12).alias("prev_seq_no"),
    )
)

filtered = (
    with_windows
    .filter(F.col("status").isin(["CANCELLED", "PENDING"]))
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "shipping_fee_whole",
        "prev_shipping_fee",
        "discount_avg_5",
        "discount_avg_3",
        "seq_no",
        "row_seq",
        "discount_avg_3_avg_3",
        "prev_seq_no",
        F.coalesce("row_seq", F.lit(0)).alias("row_seq_filled"),
    )
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w13))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .groupBy("customer_id", F.date_trunc("YEAR", "order_date").alias("year_start"))
    .agg(
        F.count("*").alias("cnt"),
        F.max("shipping_fee_whole").alias("max_shipping_fee_whole"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_rows"),
    )
    .filter(F.col("cnt") > 1)
)
