from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_month_site_id = Window.partitionBy(F.month("read_at_year"), "site_id")
w_quality_read_at_to_date_by_v = Window.partitionBy("quality", F.to_date("read_at")).orderBy(F.col("voltage").desc(), "id")
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_quality_read_at_to_date_by_v_2 = Window.partitionBy("quality", F.to_date("read_at")).orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_meter_readings
    .filter(~F.col("quality").isin("EST", "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.count(F.lit(1)).over(w_read_at_year_month_site_id).alias("rows_in_read_at_year_month_site_id"),
        (F.col("kwh") - F.lag("kwh").over(w_quality_read_at_to_date_by_v)).alias("kwh_change"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_read_at)).alias("voltage_change"),
        F.last("read_at").over(w_quality_read_at_to_date_by_v_2).alias("last_read_at"),
    )
)

enriched = (
    with_windows
    .filter(F.col("read_at") < F.lit("2025-03-08 10:00:00").cast("timestamp"))
    .select(
        "*",
        F.least(F.col("kwh"), F.col("voltage_change")).alias("least_kwh_voltage_change"),
        F.date_format(F.col("read_at_year"), "MMM yyyy").alias("read_at_year_label"),
    )
)

result = (
    enriched
    .cube("site_id", "quality")
    .agg(
        F.grouping_id().alias("gid"),
        F.percentile_approx("rows_in_read_at_year_month_site_id", 0.5).alias("median_rows_in_read_at_year_month_site_id"),
        F.round(F.sum("voltage") / F.nullif(F.sum("kwh_change"), F.lit(0)), 3).alias("voltage_to_kwh_change_ratio"),
        F.min("read_at").alias("first_read_at"),
    )
    .orderBy(F.col("site_id").asc_nulls_last(), F.col("quality").asc_nulls_last())
)
