from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost_whole", "id")
w_hist = Window.partitionBy("order_id").orderBy("shipped_date_month_end", "id")
w3 = Window.partitionBy("carrier").orderBy(F.col("cost_whole").desc(), "id")
w4 = Window.partitionBy("channel", "status").orderBy("discount", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("carrier", "order_id").orderBy(F.col("shipped_date_month_end").desc(), "id")
w6 = Window.partitionBy(F.month("delivered_date"), "channel").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy("cost_whole_bucket")

valid = (
    df_shipments
    .filter(F.month("delivered_date").isin([3, 6, 11]) & (F.col("shipped_date") >= F.lit("2023-08-24").cast("date")))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "status",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
        F.floor("cost").alias("cost_whole"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_filled"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id").eqNullSafe(F.col("d.id")), "inner")
    .select("a.*", F.col("d.discount"), F.col("d.channel"), F.col("d.order_date"))
)

with_windows = (
    with_dims
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost_whole",
        "carrier_filled",
        "discount",
        "channel",
        "order_date",
        F.lag("discount", 1).over(w).alias("prev_discount"),
        F.lag("discount", 2, 0).over(w_hist).alias("prev_discount_2"),
        F.ntile(3).over(w3).alias("cost_whole_bucket"),
        F.avg("discount").over(w4).alias("discount_avg_3_prior"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "order_id",
        "discount",
        "prev_discount",
        "prev_discount_2",
        "discount_avg_3_prior",
        F.first("prev_discount_2", ignorenulls=True).over(w6).alias("first_known_prev_discount_2"),
        F.percent_rank().over(w7).alias("cost_whole_bucket_percent_rank"),
    )
)

joined = (
    metrics
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.customer_id") == 32)
    .select(
        "src.id",
        "src.order_id",
        F.col("p.amount"),
        F.col("p.order_date"),
        F.col("p.status"),
    )
)

result = (
    joined
    .select("id", "amount")
    .orderBy(F.col("id").desc())
)
