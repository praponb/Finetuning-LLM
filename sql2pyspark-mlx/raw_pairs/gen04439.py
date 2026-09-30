from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("shipped_date", "id").rowsBetween(-6, Window.currentRow)
w_running = Window.partitionBy(F.date_trunc("week", "shipped_date"), "carrier").orderBy(F.col("cost").desc_nulls_last())
w4 = Window.partitionBy("status").orderBy("cumulative_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("running_min_status_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("cost_avg_6", "id")

scored = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.sum("cost").over(w).alias("cumulative_cost"),
        F.avg("cost").over(w2).alias("cost_avg_6"),
        F.dense_rank().over(w_running).alias("cost_dense_rank"),
    )
)

valid = (
    scored
    .filter(F.col("delivered_date") > F.to_date(F.lit("2023-03-31")))
    .select(
        "id",
        "delivered_date",
        "status",
        "cumulative_cost",
        "cost_avg_6",
        "cost_dense_rank",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
        F.length("status").alias("status_length"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.sum("status_length").over(w4).alias("running_sum_status_length"),
        F.count("status_length").over(w4).alias("running_count_status_length"),
        F.min("status_length").over(w4).alias("running_min_status_length"),
        F.max("status_length").over(w4).alias("running_max_status_length"),
        F.last("cost_dense_rank", ignorenulls=True).over(w4).alias("last_known_cost_dense_rank"),
    )
)

scored_shipments = (
    enriched
    .withColumn("first_known_cumulative_cost", F.first("cumulative_cost", ignorenulls=True).over(w5))
    .withColumn("prev_status_length", F.lag("status_length", 2).over(w_part))
)

result = (
    scored_shipments
    .withColumn("quarter_start", F.date_trunc("QUARTER", "delivered_date_month_end"))
    .withColumn("running_sum_status_length_tier", F.when(F.col("running_sum_status_length") >= 340, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["status", "quarter_start", "running_sum_status_length_tier"], ["status", "quarter_start"], ["status"]],
        "status",
        "quarter_start",
        "running_sum_status_length_tier",
    )
    .agg(
        F.percentile_approx("running_sum_status_length", 0.95).alias("running_sum_status_length_p95"),
        F.percentile_approx("cost_dense_rank", [0.25, 0.5, 0.75]).alias("cost_dense_rank_quartiles"),
        F.count_if(F.col("status") == "LOST").alias("lost_events"),
        F.round(F.avg("cumulative_cost"), 2).alias("avg_cumulative_cost"),
        F.round(F.sum("first_known_cumulative_cost") / F.nullif(F.count(F.when(F.col("status") == "LOST", 1)), F.lit(0)), 2).alias("first_known_cumulative_cost_per_lost"),
        F.max(F.when(F.col("status") == "RETURNED", F.col("last_known_cost_dense_rank"))).alias("returned_max_last_known_cost_dense_rank"),
    )
)
