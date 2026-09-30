from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_full = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_running = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_or_unknown_holder_id = Window.partitionBy("status_or_unknown", "holder_id")
w_status_or_unknown_by_second = Window.partitionBy("status_or_unknown").orderBy(F.col("second_premium_filled").desc())
w_status_or_unknown_by_second_2 = Window.partitionBy("status_or_unknown").orderBy("second_premium_filled", "id")

recent = (
    df_policies
    .filter((F.col("premium") < 2945.9) & (F.col("status") == "LAPSED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

windowed = (
    recent
    .select(
        "id",
        "start_date",
        "premium_filled",
        "status_or_unknown",
        F.nth_value("premium_filled", 2).over(w_all_by_start_date_full).alias("second_premium_filled"),
        F.min("premium").over(w_all_by_start_date_running).alias("min_premium_so_far"),
        F.round(F.avg("premium_filled").over(w_status_or_unknown_holder_id), 2).alias("status_or_unknown_holder_id_avg_premium_filled"),
        F.round(F.col("premium_filled") - F.avg("premium_filled").over(w_status_or_unknown_holder_id), 2).alias("premium_filled_vs_status_or_unknown_holder_id_av"),
    )
)

metrics = (
    windowed
    .select(
        "id",
        "start_date",
        "status_or_unknown",
        "second_premium_filled",
        "status_or_unknown_holder_id_avg_premium_filled",
        "premium_filled_vs_status_or_unknown_holder_id_av",
        F.rank().over(w_status_or_unknown_by_second).alias("second_premium_filled_rank"),
        (F.col("premium_filled") - F.lag("premium_filled").over(w_status_or_unknown_by_second_2)).alias("premium_filled_change"),
    )
)

top_per_group = (
    metrics
    .filter(F.col("second_premium_filled_rank") == 1)
    .drop("second_premium_filled_rank")
)

result = (
    top_per_group
    .filter(F.col("status_or_unknown").isin("UNKNOWN", "CANCELLED", "LAPSED"))
    .select(
        "id",
        "start_date",
        "status_or_unknown",
        "second_premium_filled",
        "status_or_unknown_holder_id_avg_premium_filled",
        "premium_filled_vs_status_or_unknown_holder_id_av",
        F.quarter(F.col("start_date")).alias("start_date_quarter"),
        F.substring(F.trim(F.col("status_or_unknown")), 1, 3).alias("status_or_unknown_prefix"),
    )
    .orderBy("status_or_unknown", F.col("start_date_quarter").asc())
)
