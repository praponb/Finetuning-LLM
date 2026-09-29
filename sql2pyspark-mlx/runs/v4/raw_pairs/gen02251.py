from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("sqft", "id")
w2 = Window.partitionBy("status").orderBy("shipping_fee", "id")
w3 = Window.partitionBy("customer_id", "channel").orderBy(F.col("shipping_fee").desc_nulls_last())
w4 = Window.partitionBy("status").orderBy("prev_amount")
w5 = Window.partitionBy("store_id").orderBy(F.col("order_date").desc(), "id")
w6 = Window.partitionBy("channel", "customer_id").orderBy("discount", "id").rowsBetween(-2, Window.currentRow)

enriched = (
    df_orders
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.opened_date"), F.col("d.sqft"), F.col("d.store_name"))
)

metrics = (
    enriched
    .withColumn("sqft_bucket", F.ntile(5).over(w))
    .withColumn("prev_amount", F.lag("amount", 1).over(w2))
    .withColumn("shipping_fee_rank", F.rank().over(w3))
)

enriched_orders = (
    metrics
    .withColumn("prev_amount_rank", F.rank().over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("prev_amount_avg_2", F.avg("prev_amount").over(w6))
)

valid = (
    enriched_orders
    .filter(F.col("order_date") > F.lit("2023-04-29").cast("date"))
    .select(
        "*",
        F.round("prev_amount_rank", 1).alias("prev_amount_rank_rounded"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

result = (
    valid
    .groupBy(
        "customer_id",
        F.date_trunc("QUARTER", "order_date").alias("quarter_start"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.count("*").alias("event_count"),
        F.percentile_approx("prev_amount_avg_2", [0.25, 0.5, 0.75]).alias("prev_amount_avg_2_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("shipping_fee_rank").alias("total_shipping_fee_rank"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("quarter_start").desc_nulls_first())
)
