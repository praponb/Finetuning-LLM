from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("order_date"), "product_id")
w2 = Window.partitionBy(F.month("order_date"), "order_id")
w_ordered = Window.partitionBy("product_id").orderBy("order_date_month_stddev_samp_amount", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy("order_date_month_stddev_samp_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("order_date_month_stddev_samp_amount", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.orderBy(F.col("order_date_month_stddev_samp_amount").desc(), "id")
w7 = Window.orderBy("amount_share", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.orderBy("last4_count_order_date_month_stddev_samp_amount", "id")

with_dims = (
    df_order_items
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.unit_price",
        F.col("p.order_date"),
        F.col("p.amount"),
    )
)

metrics = (
    with_dims
    .select(
        "id",
        "product_id",
        "order_date",
        F.stddev_samp("amount").over(w).alias("order_date_month_stddev_samp_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "order_date_month_stddev_samp_amount",
        "amount_share",
        F.min("order_date_month_stddev_samp_amount").over(w_ordered).alias("last4_min_order_date_month_stddev_samp_amount"),
        F.count("order_date_month_stddev_samp_amount").over(w_ordered).alias("last4_count_order_date_month_stddev_samp_amount"),
        F.avg("order_date_month_stddev_samp_amount").over(w_ordered).alias("last4_avg_order_date_month_stddev_samp_amount"),
        F.sum("order_date_month_stddev_samp_amount").over(w_ordered).alias("last4_sum_order_date_month_stddev_samp_amount"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "amount_share",
        "last4_avg_order_date_month_stddev_samp_amount",
        F.sum("amount_share").over(w4).alias("cumulative_amount_share"),
        F.avg("amount_share").over(w5).alias("amount_share_avg_2"),
        F.row_number().over(w6).alias("row_seq"),
        F.avg("order_date_month_stddev_samp_amount").over(w7).alias("order_date_month_stddev_samp_amount_avg_2"),
        F.lag("last4_sum_order_date_month_stddev_samp_amount", 1).over(w8).alias("prev_last4_sum_order_date_month_stddev_samp_amount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "amount_share",
        "cumulative_amount_share",
        "amount_share_avg_2",
        "row_seq",
        "order_date_month_stddev_samp_amount_avg_2",
        "prev_last4_sum_order_date_month_stddev_samp_amount",
    )
)
