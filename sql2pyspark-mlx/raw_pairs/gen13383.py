from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_first())
w2 = Window.partitionBy("status", "carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("cost_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("cost_percent_rank", "id")
w6 = Window.partitionBy("status")
w7 = Window.partitionBy("status").orderBy("cumulative_cost", "id")
w8 = Window.partitionBy(F.date_trunc("month", "shipped_date"), "status").orderBy("cost_percent_rank", "id")
w9 = Window.partitionBy(F.year("shipped_date"), "status")
w10 = Window.partitionBy("status").orderBy(F.col("cumulative_cost_bucket").desc(), "id")
w11 = Window.partitionBy("status").orderBy("prev_running_cost", "id")

enriched = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.percent_rank().over(w).alias("cost_percent_rank"),
        F.sum("cost").over(w2).alias("cumulative_cost"),
        F.sum("cost").over(w3).alias("running_cost"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.sum("cost_percent_rank").over(w4).alias("running_cost_percent_rank"),
        F.lag("running_cost", 1).over(w5).alias("prev_running_cost"),
        F.sum("cost").over(w6).alias("status_sum_cost"),
        F.ntile(4).over(w7).alias("cumulative_cost_bucket"),
    )
)

with_windows_shipments = (
    with_windows
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "cost_percent_rank",
        "cumulative_cost",
        "prev_running_cost",
        "status_sum_cost",
        "cumulative_cost_bucket",
        F.min("running_cost_percent_rank").over(w6).alias("status_min_running_cost_percent_rank"),
        F.row_number().over(w8).alias("row_seq"),
        F.max("cumulative_cost_bucket").over(w9).alias("shipped_date_year_max_cumulative_cost_bucket"),
        F.ntile(3).over(w10).alias("cumulative_cost_bucket_bucket"),
        F.ntile(10).over(w11).alias("prev_running_cost_bucket"),
    )
)

matched_keys = (
    with_windows_shipments
    .select("id")
    .subtract(
        df_shipments
        .filter(F.year("shipped_date") >= 2025)
        .select("id")
    )
)

reconciled = (
    with_windows_shipments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("shipped_date") > "2023-04-06")
    .withColumn("row_seq_per_cost_percent_rank", (F.col("row_seq") / F.nullif("cost_percent_rank", F.lit(0))))
    .withColumn("greatest_shipped_date_year_max_cumulative_cost_bucket_status_sum_cost", F.greatest("shipped_date_year_max_cumulative_cost_bucket", "status_sum_cost"))
)

result = (
    cleaned
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "cost_percent_rank",
        "cumulative_cost",
        "cumulative_cost_bucket",
        "shipped_date_year_max_cumulative_cost_bucket",
        F.when(F.col("status").rlike("^(RETURNED|IN_TRANSIT)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy(F.col("status_zone").desc_nulls_first())
)
