from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id")
w_ordered = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("order_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_shipments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_id", "carrier", "shipped_date", "delivered_date", "cost")
)

joined = (
    ranked
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.carrier",
        "a.shipped_date",
        "a.delivered_date",
        F.col("p.channel"),
        F.col("p.order_date"),
    )
)

filtered = (
    joined
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        (F.datediff("delivered_date", "order_date") / 7).alias("weeks_order_date_to_delivered_date"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

valid = (
    filtered
    .filter(F.col("shipped_date") > F.to_date(F.lit("2023-03-16")))
    .withColumn("order_date_period", F.date_trunc("week", "order_date"))
    .withColumn("channel_title", F.initcap(F.trim("channel")))
)

enriched = (
    valid
    .select(
        "*",
        F.max("weeks_order_date_to_delivered_date").over(w_ordered).alias("running_max_weeks_order_date_to_delivered_date"),
        F.count("weeks_order_date_to_delivered_date").over(w_ordered).alias("running_count_weeks_order_date_to_delivered_date"),
        F.min("weeks_order_date_to_delivered_date").over(w_ordered).alias("running_min_weeks_order_date_to_delivered_date"),
        F.avg("weeks_order_date_to_delivered_date").over(w_ordered).alias("running_avg_weeks_order_date_to_delivered_date"),
        F.first("channel", ignorenulls=True).over(w3).alias("first_known_channel"),
    )
)

result = (
    enriched
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .groupingSets(
        [["order_channel", "shipment_channel"], ["order_channel"], []],
        F.col("o.channel").alias("order_channel"),
        F.col("e.channel").alias("shipment_channel"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.weeks_order_date_to_delivered_date")).alias("max_weeks_order_date_to_delivered_date"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy("rows_total")
)
