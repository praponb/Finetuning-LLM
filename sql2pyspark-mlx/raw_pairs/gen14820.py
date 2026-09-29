from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").asc_nulls_last())
w2 = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy(F.col("running_cost_avg_3").desc(), "id")
w_recent = Window.orderBy("delivered_date", "id").rowsBetween(-5, Window.currentRow)
w_ordered = Window.orderBy("cumulative_cost_percent_rank", "id")
w9 = Window.partitionBy(F.date_trunc("week", "delivered_date"))

base = (
    df_shipments
    .filter((~F.col("carrier").eqNullSafe(F.lit("FedEx"))) & F.col("order_id").isNotNull())
    .withColumn("cost_tier", F.when(F.col("cost") >= 99, "High").when(F.col("cost") >= 32, "Medium").otherwise("Low"))
)

scored = (
    base
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        F.percent_rank().over(w).alias("cost_percent_rank"),
        F.sum("cost").over(w2).alias("running_cost"),
        F.avg("cost").over(w3).alias("order_id_avg_cost"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost_percent_rank",
        "running_cost",
        F.sum("running_cost").over(w4).alias("running_running_cost"),
        F.avg("running_cost").over(w5).alias("running_cost_avg_3"),
        F.sum("cost_percent_rank").over(w2).alias("cumulative_cost_percent_rank"),
    )
)

metrics = (
    with_windows
    .filter(F.col("delivered_date") <= "2023-08-01")
    .withColumn("running_cost_avg_3_bucket", F.ntile(10).over(w6))
    .withColumn("cost_percent_rank_avg_5", F.avg("cost_percent_rank").over(w_recent))
    .withColumn("next_cost_percent_rank", F.lead("cost_percent_rank", 1, 0).over(w_ordered))
    .withColumn("delivered_date_period_min_cumulative_cost_percent_rank", F.min("cumulative_cost_percent_rank").over(w9))
    .withColumn("row_seq", F.row_number().over(w_ordered))
)

valid = (
    metrics
    .filter(F.col("delivered_date") > F.lit("2023-09-21").cast("date"))
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "running_cost",
        "running_running_cost",
        "running_cost_avg_3",
        "cumulative_cost_percent_rank",
        "running_cost_avg_3_bucket",
        "cost_percent_rank_avg_5",
        "delivered_date_period_min_cumulative_cost_percent_rank",
        "row_seq",
        F.round("running_cost", 1).alias("running_cost_rounded"),
        F.least("row_seq", "next_cost_percent_rank").alias("least_row_seq_next_cost_percent_rank"),
    )
)

result = (
    valid
    .select(
        "id",
        "delivered_date",
        "running_cost_avg_3",
        "running_cost_avg_3_bucket",
        "cost_percent_rank_avg_5",
        "delivered_date_period_min_cumulative_cost_percent_rank",
        "running_cost_rounded",
        "least_row_seq_next_cost_percent_rank",
    )
    .orderBy(F.col("cost_percent_rank_avg_5").desc(), F.col("delivered_date").desc())
)
