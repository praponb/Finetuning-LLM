from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id", "carrier").orderBy("shipped_date_minus_81d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("running_sum_cost").desc(), "id")
w4 = Window.orderBy(F.col("running_min_cost").desc(), "id")
w5 = Window.partitionBy(F.date_trunc("month", "delivered_date"))
w6 = Window.orderBy("running_sum_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-05-08"))) & (F.col("cost") <= 63))
    .withColumn("carrier_status_key", F.concat_ws("-", "carrier", "status"))
)

base_shipments = (
    base
    .filter(F.col("delivered_date") >= F.to_date(F.lit("2023-03-29")))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "carrier_status_key",
        F.date_sub("shipped_date", 81).alias("shipped_date_minus_81d"),
    )
)

with_windows = (
    base_shipments
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "carrier_status_key",
        F.sum("cost").over(w_ordered).alias("running_sum_cost"),
        F.min("cost").over(w_ordered).alias("running_min_cost"),
        F.avg("cost").over(w_ordered).alias("running_avg_cost"),
    )
)

with_windows_shipments = (
    with_windows
    .select(
        "id",
        "delivered_date",
        "carrier_status_key",
        "running_sum_cost",
        "running_min_cost",
        F.lag("running_min_cost").over(w2).alias("prev_running_min_cost"),
        F.lead("running_min_cost").over(w2).alias("next_running_min_cost"),
        F.lead("running_sum_cost").over(w2).alias("next_running_sum_cost"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(5).over(w3).alias("running_sum_cost_bucket"),
    )
)

scored = (
    with_windows_shipments
    .select(
        "id",
        "running_sum_cost",
        "running_sum_cost_bucket",
        F.ntile(3).over(w4).alias("running_min_cost_bucket"),
        F.avg("prev_running_min_cost").over(w5).alias("delivered_date_period_avg_prev_running_min_cost"),
        F.first("running_sum_cost_bucket", ignorenulls=True).over(w6).alias("first_known_running_sum_cost_bucket"),
    )
)

totals = (
    scored
    .groupBy(
        F.when(F.col("running_sum_cost") >= 470, "PREMIUM").otherwise("STANDARD").alias("running_sum_cost_tier"),
    )
    .agg(
        F.round(F.avg("running_sum_cost_bucket"), 2).alias("avg_running_sum_cost_bucket"),
        F.percentile_approx("running_sum_cost_bucket", 0.9).alias("running_sum_cost_bucket_p90"),
        F.round(F.avg("running_sum_cost_bucket"), 2).alias("avg_running_sum_cost_bucket_2"),
        F.percentile_approx("first_known_running_sum_cost_bucket", [0.25, 0.5, 0.75]).alias("first_known_running_sum_cost_bucket_quartiles"),
    )
)

result = (
    totals
    .select("running_sum_cost_tier")
    .orderBy("running_sum_cost_tier")
    .limit(20)
)
