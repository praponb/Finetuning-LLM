from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("customer_id", "promo").orderBy("price", "id", "pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("customer_id").orderBy("running_avg_pos", "id", "pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("pos", "id", "pos")
w4 = Window.orderBy("running_min_pos", "id", "pos").rowsBetween(-6, Window.currentRow)
w5 = Window.orderBy("event_seq", "id", "pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "item"))
    .select(
        "id",
        "customer_id",
        "pos",
        F.col("item.product_id"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

metrics = (
    flattened
    .select(
        "id",
        "customer_id",
        "pos",
        F.sum("pos").over(w_ordered).alias("running_sum_pos"),
        F.max("pos").over(w_ordered).alias("running_max_pos"),
        F.avg("pos").over(w_ordered).alias("running_avg_pos"),
        F.min("pos").over(w_ordered).alias("running_min_pos"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "pos",
        "running_sum_pos",
        "running_max_pos",
        "running_avg_pos",
        "running_min_pos",
        F.sum("pos").over(w_hist).alias("running_sum_pos_2"),
        F.avg("pos").over(w_hist).alias("running_avg_pos_2"),
        F.count("pos").over(w_hist).alias("running_count_pos"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

metrics_carts = (
    with_windows
    .filter(F.col("event_seq") < 5)
    .withColumn("last7_count_running_max_pos", F.count("running_max_pos").over(w4))
    .withColumn("last7_sum_running_max_pos", F.sum("running_max_pos").over(w4))
    .withColumn("last7_avg_running_max_pos", F.avg("running_max_pos").over(w4))
)

enriched = (
    metrics_carts
    .withColumn("running_avg_running_sum_pos_2", F.avg("running_sum_pos_2").over(w5))
    .withColumn("running_sum_running_sum_pos_2", F.sum("running_sum_pos_2").over(w5))
    .withColumn("running_count_running_sum_pos_2", F.count("running_sum_pos_2").over(w5))
)

result = (
    enriched
    .select(
        "id",
        "pos",
        "running_min_pos",
        "running_avg_pos_2",
        "running_count_pos",
        "event_seq",
        "last7_sum_running_max_pos",
        "last7_avg_running_max_pos",
        "running_sum_running_sum_pos_2",
    )
)
