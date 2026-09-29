from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status").orderBy("shipped_date_plus_months", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("last3_count_cost", "id")
w3 = Window.orderBy("last3_max_cost", "id")
w4 = Window.orderBy("last3_min_cost", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy(F.date_trunc("month", "shipped_date_month_end")).orderBy("last3_max_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("last3_min_cost", "id")
w7 = Window.orderBy(F.col("last_known_last3_min_cost").desc(), "id")

valid = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-06-15").cast("date")) & (F.year("shipped_date") == 2024))
    .select(
        "*",
        F.add_months("shipped_date", 6).alias("shipped_date_plus_months"),
        F.concat_ws("/", "carrier", "status").alias("carrier_status_key"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "shipped_date",
        "cost",
        F.max("cost").over(w_running).alias("last3_max_cost"),
        F.count("cost").over(w_running).alias("last3_count_cost"),
        F.min("cost").over(w_running).alias("last3_min_cost"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("shipped_date") > F.lit("2023-03-13").cast("date"))
    .select("*", F.last_day("shipped_date").alias("shipped_date_month_end"))
)

enriched = (
    cleaned
    .select(
        "*",
        F.ntile(4).over(w2).alias("last3_count_cost_bucket"),
        F.lead("cost", 1, 0).over(w3).alias("next_cost"),
        F.avg("last3_max_cost").over(w4).alias("last3_max_cost_avg_6_prior"),
        F.last("last3_min_cost", ignorenulls=True).over(w5).alias("last_known_last3_min_cost"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

cleaned_shipments = (
    enriched
    .filter(F.col("row_seq") == 5)
    .select("*", F.round("last3_count_cost", 0).alias("last3_count_cost_rounded"))
)

with_windows_shipments = (
    cleaned_shipments
    .filter(F.col("last3_max_cost").between(54, 91))
    .select(
        "id",
        "shipped_date",
        "last3_max_cost",
        "last3_min_cost",
        "last3_count_cost_bucket",
        "last3_max_cost_avg_6_prior",
        "row_seq",
        "last3_count_cost_rounded",
        F.lead("last3_count_cost", 1).over(w3).alias("next_last3_count_cost"),
        F.ntile(5).over(w7).alias("last_known_last3_min_cost_bucket"),
    )
)

result = (
    with_windows_shipments
    .groupBy(F.date_trunc("WEEK", "shipped_date").alias("week_start"))
    .agg(
        F.percentile_approx("last3_min_cost", 0.95).alias("last3_min_cost_p95"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("last3_max_cost"), 2).alias("avg_last3_max_cost"),
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)
