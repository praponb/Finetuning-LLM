from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy("delivered_date", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("channel", "carrier").orderBy(F.col("shipping_fee").desc(), "id")
w5 = Window.partitionBy("activity_month").orderBy(F.col("event_count").desc())

filtered = (
    df_shipments
    .filter(F.col("shipped_date") >= F.lit("2023-12-13").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

enriched = (
    filtered
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.shipping_fee"), F.col("o.amount"), F.col("o.channel"))
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    ranked
    .select(
        "*",
        F.avg("amount").over(w2).alias("amount_avg_6"),
        F.avg("amount").over(w3).alias("amount_avg_6_2"),
        F.ntile(4).over(w4).alias("shipping_fee_bucket"),
    )
)

totals = (
    scored
    .groupBy(
        F.date_trunc("month", "delivered_date").alias("activity_month"),
        F.date_trunc("week", "delivered_date").alias("week_start"),
    )
    .agg(
        F.round(F.avg("amount_avg_6"), 2).alias("avg_amount_avg_6"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") < 1)
    .withColumn("event_count_dense_rank", F.dense_rank().over(w5))
)

result = (
    totals
    .select(
        "activity_month",
        "week_start",
        "avg_amount_avg_6",
        "distinct_ids",
        "event_count_dense_rank",
    )
    .orderBy(F.col("distinct_ids").desc())
)
