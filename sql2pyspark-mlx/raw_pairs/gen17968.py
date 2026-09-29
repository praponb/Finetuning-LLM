from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id", "status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("order_id").orderBy("running_min_cost", "id")
w_hist = Window.partitionBy("order_id")

metrics = (
    df_shipments
    .select(
        "id",
        "order_id",
        F.count("cost").over(w_ordered).alias("running_count_cost"),
        F.min("cost").over(w_ordered).alias("running_min_cost"),
        F.sum("cost").over(w_ordered).alias("running_sum_cost"),
    )
)

valid = (
    metrics
    .filter(F.col("order_id") == 1)
    .select(
        "id",
        "order_id",
        "running_count_cost",
        "running_min_cost",
        "running_sum_cost",
        F.when(F.col("running_min_cost") >= 76, "High").when(F.col("running_min_cost") >= 30, "Medium").otherwise("Low").alias("running_min_cost_level"),
    )
)

scored = (
    valid
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "order_id",
        "running_count_cost",
        "running_sum_cost",
        "running_min_cost_level",
        F.lag("running_count_cost", 1).over(w_running).alias("prev_running_count_cost"),
        F.avg("running_count_cost").over(w_hist).alias("order_id_avg_running_count_cost"),
        F.min("running_sum_cost").over(w_hist).alias("order_id_min_running_sum_cost"),
    )
)

with_dims = (
    scored
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.running_count_cost",
        "s.running_sum_cost",
        "s.running_min_cost_level",
        "s.prev_running_count_cost",
        "s.order_id_avg_running_count_cost",
        "s.order_id_min_running_sum_cost",
        F.col("p.channel"),
        F.col("p.order_date"),
    )
)

result = (
    with_dims
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .groupingSets(
        [["status", "order_channel", "shipment_channel"], ["status", "order_channel"], ["status"], []],
        F.col("o.status").alias("status"),
        F.col("o.channel").alias("order_channel"),
        F.col("a.channel").alias("shipment_channel"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("a.prev_running_count_cost")).alias("avg_prev_running_count_cost"),
        F.max(F.col("a.running_sum_cost")).alias("max_running_sum_cost"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("avg_prev_running_count_cost")
)
