from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("cost").desc_nulls_first(), "id")
w2 = Window.partitionBy("order_id")
w_hist = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("rn", "id")
w5 = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w7 = Window.partitionBy("carrier").orderBy(F.col("cost").asc_nulls_last(), "id")
w8 = Window.partitionBy("carrier").orderBy(F.col("cumulative_cost").desc_nulls_last(), "id")
w9 = Window.orderBy("running_cost", "id")
w10 = Window.orderBy(F.col("prev_cost").desc(), "id")
w11 = Window.orderBy("delivered_date", "id")

latest = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    latest
    .filter(F.col("cost").between(59, 90))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.sum("cost").over(w_hist).alias("cumulative_cost"),
        F.lag("cost", 1).over(w4).alias("prev_cost"),
        F.sum("cost").over(w5).alias("running_cost"),
        F.lead("cost", 1, 0).over(w6).alias("next_cost"),
    )
)

ranked = (
    with_windows
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest_shipments = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "shipped_date", "delivered_date", "cost", "prev_cost", "running_cost")
)

filtered = (
    latest_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-06-27").cast("date")) & F.col("cost").between(37, 71))
    .withColumn("days_delivered_date_to_shipped_date", F.datediff("shipped_date", "delivered_date"))
)

valid = (
    filtered
    .filter((F.col("delivered_date") > F.lit("2023-06-05").cast("date")) & (F.col("days_delivered_date_to_shipped_date") >= 39))
    .withColumn("shipped_date_dayofweek", F.dayofweek("shipped_date"))
)

metrics = (
    valid
    .select(
        "*",
        F.lead("prev_cost", 2).over(w9).alias("next_prev_cost"),
        F.ntile(3).over(w10).alias("prev_cost_bucket"),
        F.lead("running_cost", 1).over(w11).alias("next_running_cost"),
    )
)

summary = (
    metrics
    .groupBy(F.date_trunc("WEEK", "delivered_date").alias("week_start"))
    .agg(
        F.percentile_approx("running_cost", 0.95).alias("running_cost_p95"),
        F.round(F.avg("next_running_cost"), 2).alias("avg_next_running_cost"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    summary
    .select("week_start", "avg_next_running_cost", "distinct_ids")
    .orderBy(F.col("week_start").asc_nulls_first())
)
