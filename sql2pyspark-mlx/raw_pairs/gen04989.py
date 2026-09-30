from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status")
w_part = Window.partitionBy("status").orderBy("cost", "id")
w4 = Window.orderBy(F.col("shipping_fee_rounded").desc(), "id")
w5 = Window.partitionBy("status").orderBy(F.col("last_known_amount").desc(), "id")
w6 = Window.orderBy("shipping_fee_rounded_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.amount") < 450)
    .select("a.*", F.col("p.amount"), F.col("p.discount"), F.col("p.order_date"))
)

enriched = (
    joined
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.cost",
        "a.status",
        "a.amount",
        "a.order_date",
        F.col("b.shipping_fee"),
    )
)

filtered = (
    enriched
    .filter(F.col("status").isin(["RETURNED", "IN_TRANSIT", "LOST"]))
    .withColumn("order_date_label", F.date_format("order_date", "MMM yyyy"))
    .withColumn("shipping_fee_rounded", F.round("shipping_fee", 1))
)

metrics = (
    filtered
    .select(
        "id",
        "order_id",
        "cost",
        "status",
        "order_date",
        "shipping_fee_rounded",
        F.last("amount", ignorenulls=True).over(w).alias("last_known_amount"),
        F.stddev_samp("cost").over(w2).alias("status_stddev_samp_cost"),
        F.lead("shipping_fee_rounded", 1, 0).over(w_part).alias("next_shipping_fee_rounded"),
    )
)

filtered_shipments = (
    metrics
    .filter(F.col("cost").between(41, 73))
    .select(
        "id",
        "cost",
        "status",
        "shipping_fee_rounded",
        "last_known_amount",
        "status_stddev_samp_cost",
        "next_shipping_fee_rounded",
        (F.col("cost") - F.col("shipping_fee_rounded")).alias("cost_minus_shipping_fee_rounded"),
        F.coalesce("next_shipping_fee_rounded", F.lit(0)).alias("next_shipping_fee_rounded_filled"),
    )
)

metrics_shipments = (
    filtered_shipments
    .select(
        "id",
        "cost",
        "next_shipping_fee_rounded_filled",
        F.ntile(4).over(w4).alias("shipping_fee_rounded_bucket"),
        F.ntile(3).over(w5).alias("last_known_amount_bucket"),
    )
)

result = (
    metrics_shipments
    .select(
        "id",
        "cost",
        "next_shipping_fee_rounded_filled",
        "shipping_fee_rounded_bucket",
        "last_known_amount_bucket",
        F.avg("cost").over(w6).alias("running_avg_cost"),
        F.count("cost").over(w6).alias("running_count_cost"),
        F.sum("cost").over(w6).alias("running_sum_cost"),
    )
)
