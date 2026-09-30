from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status").orderBy("shipped_date", "id")
w2 = Window.partitionBy(F.year("delivered_date")).orderBy(F.col("shipped_date").desc(), "id")
w3 = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("order_date", "id")
w5 = Window.partitionBy("carrier").orderBy(F.col("shipping_fee").desc_nulls_last())

filtered = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-06-16"))) & F.col("carrier").isNotNull())
    .select("*", F.last_day("shipped_date").alias("shipped_date_month_end"))
)

metrics = (
    filtered
    .withColumn("next_cost", F.lead("cost").over(w))
    .withColumn("prev_cost", F.lag("cost").over(w))
    .withColumn("seq_no", F.row_number().over(w))
)

cleaned = (
    metrics
    .filter((F.col("delivered_date") > F.lit("2023-03-18").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "shipped_date_month_end",
        "next_cost",
        "seq_no",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .filter(F.col("p.amount").isNotNull())
    .select("e.*", F.col("p.order_date"), F.col("p.channel"))
)

enriched_shipments = (
    enriched
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no_2"),
        F.first("seq_no", ignorenulls=True).over(w3).alias("first_known_seq_no"),
    )
)

enriched_shipments_2 = (
    enriched_shipments
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.carrier",
        "a.delivered_date",
        "a.status",
        "a.seq_no",
        "a.delivered_date_month_end",
        "a.order_date",
        "a.channel",
        "a.seq_no_2",
        "a.first_known_seq_no",
        F.col("b.shipping_fee"),
        F.col("b.channel").alias("order_channel"),
        F.col("b.order_date").alias("order_order_date"),
    )
)

result = (
    enriched_shipments_2
    .select(
        "id",
        "order_id",
        "delivered_date",
        "status",
        "delivered_date_month_end",
        "order_date",
        "channel",
        "first_known_seq_no",
        "shipping_fee",
        "order_channel",
        "order_order_date",
        F.lag("seq_no_2", 1).over(w4).alias("prev_seq_no_2"),
        F.dense_rank().over(w5).alias("shipping_fee_dense_rank"),
    )
)
