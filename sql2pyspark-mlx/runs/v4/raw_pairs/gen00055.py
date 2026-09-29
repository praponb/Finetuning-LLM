from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("order_id").orderBy("unit_price", "id")
w_running = Window.partitionBy("order_id").orderBy("unit_price_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("unit_price", "id")
w4 = Window.partitionBy("order_id").orderBy("order_date", "id")
w5 = Window.partitionBy("activity_month").orderBy(F.col("distinct_order_ids").desc())

cleaned = (
    df_order_items
    .filter(F.col("order_id") == 35)
    .select(
        "id",
        "order_id",
        "unit_price",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.row_number().over(w_hist).alias("event_seq"),
        F.sum("unit_price").over(w_running).alias("running_unit_price"),
        F.row_number().over(w3).alias("event_seq_2"),
    )
)

joined = (
    metrics
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.order_date"), F.col("o.status"))
)

scored = (
    joined
    .select(
        "*",
        F.lag("unit_price", 1).over(w4).alias("prev_unit_price"),
        F.ntile(3).over(w3).alias("unit_price_bucket"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("month", "order_date").alias("activity_month"),
        F.when(F.col("event_seq_2") >= 8, "PREMIUM").otherwise("STANDARD").alias("event_seq_2_tier"),
    )
    .agg(
        F.round(F.avg("prev_unit_price"), 2).alias("avg_prev_unit_price"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
    .withColumn("distinct_order_ids_rank", F.rank().over(w5))
)
