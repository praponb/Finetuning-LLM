from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_site_commissioned_on = Window.partitionBy("site_id", F.month("site_commissioned_on")).orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality_by_site_comm = Window.partitionBy("site_id", "quality").orderBy(F.col("site_commissioned_on").desc(), "id")
w_quality_site_commissioned_on = Window.partitionBy("quality", F.date_trunc("month", "site_commissioned_on")).orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_year_quality_by_read = Window.partitionBy(F.year("read_at"), "quality").orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_quality_by_site_comm_2 = Window.partitionBy("site_id", "quality").orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_to_date_by_second_re = Window.partitionBy(F.to_date("read_at")).orderBy("second_read_at", "id")
w_first_read_at_month_site_id = Window.partitionBy(F.month("first_read_at"), "site_id")
w_read_at_to_date_by_second_re_2 = Window.partitionBy(F.to_date("read_at")).orderBy("second_read_at")

prepared_readings = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(10, 14)) & (F.col("quality") == "BAD"))
)

combined_details = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

ranked = (
    combined_details
    .select(
        "*",
        F.last("site_commissioned_on", ignorenulls=True).over(w_site_id_site_commissioned_on).alias("last_known_site_commissioned_on"),
        F.ntile(4).over(w_site_id_quality_by_site_comm).alias("site_commissioned_on_quartile"),
        F.nth_value("site_commissioned_on", 2).over(w_quality_site_commissioned_on).alias("second_site_commissioned_on"),
        F.nth_value("read_at", 2).over(w_read_at_year_quality_by_read).alias("second_read_at"),
        F.first("read_at").over(w_site_id_quality_by_site_comm_2).alias("first_read_at"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_site_id_quality_by_site_comm))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.nth_value("read_at", 2).over(w_quality_site_commissioned_on).alias("second_read_at_2"),
        F.last("site_commissioned_on").over(w_site_id_quality_by_site_comm_2).alias("last_site_commissioned_on"),
    )
)

with_trends = (
    ranked
    .select(
        "*",
        F.round(F.percent_rank().over(w_read_at_to_date_by_second_re), 3).alias("second_read_at_percent_rank"),
        F.sum("minutes_since_prev_read_at").over(w_first_read_at_month_site_id).alias("first_read_at_month_site_id_sum_minutes_since_pr"),
        F.round(F.col("minutes_since_prev_read_at") - F.avg("minutes_since_prev_read_at").over(w_first_read_at_month_site_id), 2).alias("minutes_since_prev_read_at_vs_first_read_at_mont"),
        F.dense_rank().over(w_read_at_to_date_by_second_re_2).alias("second_read_at_dense_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "site_commissioned_on",
        "last_known_site_commissioned_on",
        "site_commissioned_on_quartile",
        "second_site_commissioned_on",
        "second_read_at",
        "first_read_at",
        "minutes_since_prev_read_at",
        "second_read_at_percent_rank",
        "first_read_at_month_site_id_sum_minutes_since_pr",
        "minutes_since_prev_read_at_vs_first_read_at_mont",
        "second_read_at_dense_rank",
    )
)
