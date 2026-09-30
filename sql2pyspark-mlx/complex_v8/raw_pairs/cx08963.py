from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_site_id = Window.partitionBy(F.month("read_at"), "site_id")
w_site_id_quality_by_read_at_t = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").asc_nulls_last())
w_quality = Window.partitionBy("quality")
w_site_id_quality_by_read_at_f = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_id_by_kwh_full = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_site_commissioned = Window.partitionBy("quality").orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_meter_readings
    .filter((F.col("voltage") >= 221.4) & (F.col("quality").isin("BAD", "OK")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

with_parent = (
    cleaned.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select("s.*", F.col("si.commissioned_on").alias("site_commissioned_on"))
)

ranked = (
    with_parent
    .select(
        "*",
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_month_site_id), 2).alias("kwh_vs_read_at_month_site_id_avg"),
        F.round(F.percent_rank().over(w_site_id_quality_by_read_at_t), 3).alias("read_at_percent_rank"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality), F.lit(0)), 4).alias("kwh_share_of_quality"),
        F.nth_value("read_at", 2).over(w_site_id_quality_by_read_at_f).alias("second_read_at"),
        F.rank().over(w_site_id_quality_by_read_at_t).alias("read_at_rank"),
        F.nth_value("kwh", 2).over(w_quality_site_id_by_kwh_full).alias("second_kwh"),
        F.sum("kwh").over(w_quality).alias("quality_sum_kwh"),
        F.last("site_commissioned_on").over(w_quality_by_site_commissioned).alias("last_site_commissioned_on"),
    )
)

top_rows = (
    ranked
    .filter(F.col("read_at_rank") <= 2)
    .drop("read_at_rank")
)

result = (
    top_rows
    .filter(F.col("site_commissioned_on") >= F.lit("2024-06-27").cast("date"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "site_commissioned_on",
        "kwh_vs_read_at_month_site_id_avg",
        "read_at_percent_rank",
        "kwh_share_of_quality",
        "second_kwh",
        "quality_sum_kwh",
        "last_site_commissioned_on",
    )
    .orderBy(F.col("site_id").desc())
)
