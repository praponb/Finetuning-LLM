from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_or_unknown_status_by = Window.partitionBy("status_or_unknown", "status").orderBy(F.col("end_date").desc(), "id")
w_status_or_unknown_status_by_2 = Window.partitionBy("status_or_unknown", "status").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_month_by_start_date = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy(F.col("start_date").desc(), "id")
w_end_date_month_by_start_date_2 = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy(F.col("start_date").desc(), "id").rowsBetween(-3, -1)
w_end_date_month_by_start_date_3 = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy(F.col("start_date").desc(), "id").rowsBetween(-6, Window.currentRow)

prepared = (
    df_policies
    .filter(F.col("status") == "LAPSED")
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

with_windows = (
    prepared
    .select(
        "*",
        F.lead("start_date", 3).over(w_status_or_unknown_status_by).alias("next3_start_date"),
        F.last("premium_bucket", ignorenulls=True).over(w_status_or_unknown_status_by_2).alias("last_known_premium_bucket"),
        F.lag("start_date", 2).over(w_end_date_month_by_start_date).alias("prev2_start_date"),
        F.max("days_since_start_date").over(w_end_date_month_by_start_date_2).alias("days_since_start_date_max_3_prior"),
        F.round(F.avg("days_since_start_date").over(w_end_date_month_by_start_date_3), 2).alias("days_since_start_date_avg_7"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["holder_id", "status_or_unknown", "status"], ["holder_id"]],
        "holder_id", "status_or_unknown", "status",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.max("days_since_start_date_max_3_prior").alias("max_days_since_start_date_max_3_prior"),
        F.min("next3_start_date").alias("first_next3_start_date"),
    )
)
