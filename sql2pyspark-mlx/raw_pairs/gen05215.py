from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("carrier", "order_id")
w4 = Window.partitionBy("carrier")
w5 = Window.partitionBy("order_id", "carrier").orderBy("delivered_date", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("order_id", "carrier").orderBy(F.col("carrier_min_cost").desc(), "id")
w7 = Window.partitionBy("carrier").orderBy(F.col("cost_share").desc(), "id")
w8 = Window.orderBy("shipped_date", "id")
w9 = Window.partitionBy("carrier").orderBy("cost_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("order_id").orderBy("running_cost_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("cost_share", "id").rowsBetween(-6, Window.currentRow)

metrics = (
    df_shipments
    .withColumn("first_known_carrier", F.first("carrier", ignorenulls=True).over(w))
    .withColumn("cost_bucket", F.ntile(4).over(w2))
    .withColumn("carrier_min_cost", F.min("cost").over(w3))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w4)), 4))
    .withColumn("cost_avg_3", F.avg("cost").over(w5))
)

scored = (
    metrics
    .select(
        "id",
        "order_id",
        "cost",
        "cost_bucket",
        "carrier_min_cost",
        "cost_share",
        F.row_number().over(w6).alias("event_seq"),
        F.ntile(3).over(w7).alias("cost_share_bucket"),
        F.lead("cost_bucket", 2).over(w8).alias("next_cost_bucket"),
        F.sum("cost_avg_3").over(w9).alias("running_cost_avg_3"),
    )
)

enriched = (
    scored
    .filter(F.col("event_seq") > 4)
    .select(
        "id",
        "cost_share",
        "running_cost_avg_3",
        F.max("carrier_min_cost").over(w_grp).alias("running_max_carrier_min_cost"),
        F.sum("carrier_min_cost").over(w_grp).alias("running_sum_carrier_min_cost"),
        F.avg("carrier_min_cost").over(w_grp).alias("running_avg_carrier_min_cost"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "cost_share",
        "running_cost_avg_3",
        "running_max_carrier_min_cost",
        "running_avg_carrier_min_cost",
        F.sum("running_sum_carrier_min_cost").over(w11).alias("last7_sum_running_sum_carrier_min_cost"),
        F.min("running_sum_carrier_min_cost").over(w11).alias("last7_min_running_sum_carrier_min_cost"),
        F.count("running_sum_carrier_min_cost").over(w11).alias("last7_count_running_sum_carrier_min_cost"),
        F.avg("running_sum_carrier_min_cost").over(w11).alias("last7_avg_running_sum_carrier_min_cost"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("running_cost_avg_3") >= 188)
    .select(
        "id",
        "cost_share",
        "running_cost_avg_3",
        "running_max_carrier_min_cost",
        "last7_sum_running_sum_carrier_min_cost",
        "last7_min_running_sum_carrier_min_cost",
        "last7_avg_running_sum_carrier_min_cost",
        F.col("last7_count_running_sum_carrier_min_cost").cast("string").alias("last7_count_running_sum_carrier_min_cost_num"),
        F.when(F.col("running_cost_avg_3") >= 538, "High").when(F.col("running_cost_avg_3") >= 69, "Medium").otherwise("Low").alias("running_cost_avg_3_band"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "running_cost_avg_3",
        "running_max_carrier_min_cost",
        "last7_sum_running_sum_carrier_min_cost",
        "last7_min_running_sum_carrier_min_cost",
        "last7_avg_running_sum_carrier_min_cost",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
