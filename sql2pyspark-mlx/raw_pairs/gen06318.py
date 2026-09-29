from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("shipped_date"), "order_id").orderBy("shipped_date", "id")
w2 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")
w4 = Window.partitionBy("status", "order_id")

base = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-07-14").cast("date")) & F.col("carrier").isNotNull())
    .withColumn("status_title", F.initcap(F.trim("status")))
    .withColumn("delivered_date_quarter", F.quarter("delivered_date"))
    .withColumn("carrier_clean", F.coalesce("carrier", F.lit("N/A")))
)

metrics = (
    base
    .filter(F.col("carrier") == "FedEx")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "status_title",
        "delivered_date_quarter",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.sum("delivered_date_quarter").over(w2).alias("cumulative_delivered_date_quarter"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.ntile(3).over(w_ordered).alias("cost_bucket"),
        F.stddev_samp("delivered_date_quarter").over(w4).alias("status_stddev_samp_delivered_date_quarter"),
    )
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.shipping_fee"), F.col("d.order_date"))
)

wide = (
    with_dims
    .groupBy("order_id")
    .pivot("status", ["DELIVERED", "IN_TRANSIT"])
    .agg(F.max("next_cost").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "order_id",
        F.col("DELIVERED_value_sum").alias("delivered_value_sum"),
        F.col("DELIVERED_n").alias("delivered_n"),
        F.col("IN_TRANSIT_value_sum").alias("in_transit_value_sum"),
        F.col("IN_TRANSIT_n").alias("in_transit_n"),
    )
)

result = (
    wide
    .select("order_id", "delivered_value_sum", "in_transit_value_sum", "in_transit_n")
    .orderBy("in_transit_n", "delivered_value_sum")
)
