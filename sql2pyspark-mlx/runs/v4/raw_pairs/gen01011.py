from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc(), "id")
w2 = Window.orderBy("cost", "id")
w3 = Window.partitionBy("carrier", "order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w5 = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id")
w7 = Window.partitionBy("status", "carrier").orderBy("first_known_cost_bucket")
w_running = Window.partitionBy("carrier", "order_id").orderBy("first_known_cost_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("carrier").orderBy(F.col("first_known_cost_bucket").desc(), "id")
w10 = Window.orderBy("prev_prev_cost", "id").rowsBetween(-6, Window.currentRow)

metrics = (
    df_shipments
    .withColumn("cost_bucket", F.ntile(4).over(w))
    .withColumn("prev_cost", F.lag("cost", 2).over(w2))
)

scored = (
    metrics
    .withColumn("cumulative_cost_bucket", F.sum("cost_bucket").over(w3))
    .withColumn("prev_prev_cost", F.lag("prev_cost", 2).over(w4))
    .withColumn("first_known_cost_bucket", F.first("cost_bucket", ignorenulls=True).over(w5))
    .withColumn("order_id_count_cost", F.count("cost").over(w6))
)

joined = (
    scored
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.channel"))
)

latest = (
    joined
    .withColumn("dedup_rank", F.rank().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.discount"))
)

enriched_shipments = (
    enriched
    .withColumn("running_cost", F.sum("cost").over(w_running))
    .withColumn("first_known_cost_bucket_bucket", F.ntile(5).over(w9))
    .withColumn("discount_avg_6", F.avg("discount").over(w10))
)

result = (
    enriched_shipments
    .groupBy("status")
    .agg(
        F.avg(F.when(F.col("carrier") == "FedEx", F.col("prev_cost"))).alias("fedex_avg_prev_cost"),
        F.max(F.when(F.col("carrier") == "FedEx", F.col("order_id_count_cost"))).alias("fedex_max_order_id_count_cost"),
        F.round(F.avg("order_id_count_cost"), 2).alias("avg_order_id_count_cost"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.round(F.avg("prev_prev_cost"), 2).alias("avg_prev_prev_cost"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count_2"),
    )
    .orderBy(F.col("avg_prev_prev_cost").desc_nulls_last())
)
