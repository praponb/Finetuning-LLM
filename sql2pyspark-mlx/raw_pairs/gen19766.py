from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("customer_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "status").orderBy("amount", "id")

valid = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-08-14").cast("date"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.lower("channel").alias("channel_lower"),
        (F.col("shipping_fee") + F.col("amount")).alias("amount_with_shipping"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "amount",
        "discount",
        "channel_lower",
        F.count("amount_with_shipping").over(w_running).alias("running_count_amount_with_shipping"),
        F.sum("amount_with_shipping").over(w_running).alias("running_sum_amount_with_shipping"),
        F.max("amount_with_shipping").over(w_running).alias("running_max_amount_with_shipping"),
        F.avg("amount_with_shipping").over(w_running).alias("running_avg_amount_with_shipping"),
    )
)

base = (
    with_windows
    .filter(F.col("discount") > 0.3)
    .select(
        "id",
        "store_id",
        "status",
        "amount",
        "discount",
        "running_count_amount_with_shipping",
        "running_sum_amount_with_shipping",
        "running_max_amount_with_shipping",
        "running_avg_amount_with_shipping",
        (F.col("running_sum_amount_with_shipping") / F.nullif("running_avg_amount_with_shipping", F.lit(0))).alias("running_sum_amount_with_shipping_per_running_avg_amount_with_shipping"),
        F.round("running_sum_amount_with_shipping", 0).alias("running_sum_amount_with_shipping_rounded"),
    )
)

enriched = (
    base
    .select(
        "id",
        "store_id",
        "status",
        "running_count_amount_with_shipping",
        "running_sum_amount_with_shipping",
        "running_max_amount_with_shipping",
        "running_avg_amount_with_shipping",
        "running_sum_amount_with_shipping_per_running_avg_amount_with_shipping",
        F.lag("running_sum_amount_with_shipping").over(w2).alias("prev_running_sum_amount_with_shipping"),
        F.lead("running_sum_amount_with_shipping").over(w2).alias("next_running_sum_amount_with_shipping"),
        F.lead("running_sum_amount_with_shipping_per_running_avg_amount_with_shipping").over(w2).alias("next_running_sum_amount_with_shipping_per_running_avg_amount_with_shipping"),
        F.lag("running_sum_amount_with_shipping_per_running_avg_amount_with_shipping").over(w2).alias("prev_running_sum_amount_with_shipping_per_running_avg_amount_with_shipping"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

result = (
    enriched
    .select(
        "id",
        "store_id",
        "running_sum_amount_with_shipping_per_running_avg_amount_with_shipping",
        "next_running_sum_amount_with_shipping",
    )
    .unpivot(
        ids=["id", "store_id"],
        values=["running_sum_amount_with_shipping_per_running_avg_amount_with_shipping", "next_running_sum_amount_with_shipping"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
)
