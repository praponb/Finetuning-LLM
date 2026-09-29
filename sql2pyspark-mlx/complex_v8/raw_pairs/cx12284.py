from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_read_at = Window.partitionBy("site_id").orderBy(F.col("read_at").asc_nulls_last(), "id")
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at_running = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_site_id = Window.partitionBy("site_id")
w_all_by_read_at_full = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_or_unknown = Window.partitionBy("quality_or_unknown")
w_quality_or_unknown_by_minute = Window.partitionBy("quality_or_unknown").orderBy(F.col("minutes_since_prev_read_at").desc(), "id")
w_next3_read_at_year_site_id = Window.partitionBy(F.year("next3_read_at"), "site_id")
w_kwh_tercile_by_read_at = Window.partitionBy("kwh_tercile").orderBy("read_at", "id")

base = (
    df_meter_readings
    .filter((F.col("voltage") >= 218.4) & (F.col("site_id") <= 7))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

with_trends = (
    base
    .select(
        "*",
        F.ntile(4).over(w_site_id_by_read_at).alias("read_at_quartile"),
        F.ntile(3).over(w_all_by_kwh).alias("kwh_tercile"),
        F.first("kwh", ignorenulls=True).over(w_all_by_kwh_running).alias("first_known_kwh"),
        F.sum("voltage").over(w_all_by_read_at_running).alias("running_voltage"),
        F.lead("read_at", 3).over(w_all_by_read_at).alias("next3_read_at"),
        F.row_number().over(w_all_by_kwh).alias("seq_no"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_kwh))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.lag("read_at").over(w_all_by_kwh).alias("prev_read_at"),
        F.first("voltage", ignorenulls=True).over(w_all_by_kwh_running).alias("first_known_voltage"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.first("read_at").over(w_all_by_read_at_full).alias("first_read_at"),
        F.max("voltage").over(w_site_id).alias("site_id_max_voltage"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.size(F.collect_set("quality").over(w_quality_or_unknown)).alias("distinct_quality_in_quality_or_unknown"),
        (F.col("kwh_share_of_site_id") - F.lag("kwh_share_of_site_id").over(w_quality_or_unknown_by_minute)).alias("kwh_share_of_site_id_change"),
        F.round(F.col("first_known_kwh") / F.nullif(F.sum("first_known_kwh").over(w_next3_read_at_year_site_id), F.lit(0)), 4).alias("first_known_kwh_share_of_next3_read_at_year_site"),
        F.row_number().over(w_kwh_tercile_by_read_at).alias("read_at_seq"),
    )
)

result = (
    metrics
    .filter(F.col("first_known_kwh_share_of_next3_read_at_year_site").between(0.1, 0.9))
    .select(
        "id",
        "kwh",
        "voltage",
        "quality_or_unknown",
        "kwh_tercile",
        "first_known_kwh",
        "seq_no",
        "minutes_since_prev_read_at",
        "prev_read_at",
        "first_known_voltage",
        "first_read_at",
        "site_id_max_voltage",
        "distinct_quality_in_quality_or_unknown",
        "kwh_share_of_site_id_change",
        "read_at_seq",
        F.round(F.col("first_known_kwh") * 1.5, 2).alias("first_known_kwh_adj"),
    )
    .orderBy("voltage")
)
