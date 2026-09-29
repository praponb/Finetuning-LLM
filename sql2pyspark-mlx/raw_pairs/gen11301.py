from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("order_date"), "channel").orderBy("delivered_date", "id")
w2 = Window.partitionBy("channel", "order_id").orderBy("cost", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy(F.date_trunc("week", "delivered_date")).orderBy("discount", "id")
w4 = Window.partitionBy(F.year("order_date"), "order_id").orderBy("carrier_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(-6, -1)
w8 = Window.orderBy(F.col("row_seq_whole").desc(), "id")
w_part = Window.partitionBy(F.date_trunc("month", "order_date"), "carrier")
w10 = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")
w11 = Window.partitionBy("carrier")
w_running = Window.partitionBy(F.date_trunc("YEAR", "shipped_date"), "carrier").orderBy("prev_cost_avg_6_prior", "id")

filtered = (
    df_shipments
    .filter(F.col("shipped_date") > F.to_date(F.lit("2023-03-29")))
    .withColumn("carrier_length", F.length("carrier"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("NONE")))
)

enriched = (
    filtered
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select("src.*", F.col("o.discount"), F.col("o.order_date"), F.col("o.channel"))
)

with_windows = (
    enriched
    .select(
        "id",
        "carrier",
        "shipped_date",
        "carrier_filled",
        "order_date",
        F.row_number().over(w).alias("row_seq"),
        F.avg("discount").over(w2).alias("discount_avg_2_prior"),
        F.lag("cost", 1, 0).over(w3).alias("prev_cost"),
        F.last("carrier_length", ignorenulls=True).over(w4).alias("last_known_carrier_length"),
        F.sum("carrier_length").over(w5).alias("running_carrier_length"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("carrier").isNotNull())
    .withColumn("row_seq_whole", F.ceil("row_seq"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.first("row_seq", ignorenulls=True).over(w_recent).alias("first_known_row_seq"),
        F.avg("prev_cost").over(w7).alias("prev_cost_avg_6_prior"),
    )
)

with_windows_shipments = (
    metrics
    .select(
        "*",
        F.row_number().over(w8).alias("row_seq_2"),
        F.count("row_seq").over(w_part).alias("order_date_period_count_row_seq"),
        F.row_number().over(w10).alias("seq_no"),
        F.max("running_carrier_length").over(w11).alias("carrier_max_running_carrier_length"),
        F.lead("discount_avg_2_prior", 1).over(w_running).alias("next_discount_avg_2_prior"),
    )
)

result = (
    with_windows_shipments
    .cube(
        F.date_trunc("MONTH", "shipped_date").alias("activity_month"),
        F.date_trunc("WEEK", "shipped_date").alias("week_start"),
    )
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", F.col("row_seq_2"))).alias("flash_total_row_seq_2"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
    )
)
