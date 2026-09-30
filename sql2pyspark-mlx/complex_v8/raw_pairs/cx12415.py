from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_or_unknown_quality_b = Window.partitionBy("quality_or_unknown", "quality").orderBy(F.col("read_at").desc())
w_quality_by_site_commissioned = Window.partitionBy("quality").orderBy(F.col("site_commissioned_on").desc(), "id")
w_site_commissioned_on_month_q = Window.partitionBy(F.date_trunc("month", "site_commissioned_on"), "quality_or_unknown")
w_quality_or_unknown_quality_b_2 = Window.partitionBy("quality_or_unknown", "quality").orderBy(F.col("read_at").desc(), "id")
w_site_id_by_voltage_plus_kwh = Window.partitionBy("site_id").orderBy(F.col("voltage_plus_kwh").desc(), "id")
w_quality_or_unknown_site_id_b = Window.partitionBy("quality_or_unknown", "site_id").orderBy("kwh", "id")
w_quality_or_unknown_site_id_b_2 = Window.partitionBy("quality_or_unknown", "site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_or_unknown_site_id_b_3 = Window.partitionBy("quality_or_unknown", "site_id").orderBy("kwh")
w_quality_or_unknown_site_id_b_4 = Window.partitionBy("quality_or_unknown", "site_id").orderBy("read_at_dense_rank", "id")
w_read_at_dense_rank_distinct = Window.partitionBy("read_at_dense_rank", "distinct_quality_in_site_commissioned_on_month_q").orderBy(F.col("prev_kwh").asc_nulls_last(), "id")
w_quality_by_last_known_site_c = Window.partitionBy("quality").orderBy("last_known_site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_meter_readings
    .filter(F.col("site_id") <= 9)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
    )
)

with_parent_site = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", F.col("si.commissioned_on").alias("site_commissioned_on"))
)

with_trends = (
    with_parent_site
    .select(
        "*",
        F.rank().over(w_quality_or_unknown_quality_b).alias("read_at_rank"),
        F.ntile(3).over(w_quality_by_site_commissioned).alias("site_commissioned_on_tercile"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_commissioned_on_month_q), 2).alias("voltage_vs_site_commissioned_on_month_quality_or"),
        F.lag("kwh").over(w_quality_or_unknown_quality_b_2).alias("prev_kwh"),
        F.ntile(10).over(w_site_id_by_voltage_plus_kwh).alias("voltage_plus_kwh_decile"),
        F.lag("voltage_plus_kwh", 3).over(w_quality_or_unknown_site_id_b).alias("prev3_voltage_plus_kwh"),
        F.last("site_commissioned_on", ignorenulls=True).over(w_quality_or_unknown_site_id_b_2).alias("last_known_site_commissioned_on"),
        F.size(F.collect_set("quality").over(w_site_commissioned_on_month_q)).alias("distinct_quality_in_site_commissioned_on_month_q"),
        F.dense_rank().over(w_quality_or_unknown_quality_b_2).alias("read_at_dense_rank"),
        F.lag("voltage").over(w_quality_by_site_commissioned).alias("prev_voltage"),
        F.rank().over(w_site_id_by_voltage_plus_kwh).alias("voltage_plus_kwh_rank"),
        F.round(F.percent_rank().over(w_quality_or_unknown_quality_b), 3).alias("read_at_percent_rank"),
        F.round(F.percent_rank().over(w_quality_or_unknown_site_id_b_3), 3).alias("kwh_percent_rank"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        (F.col("kwh") - F.lag("kwh").over(w_quality_or_unknown_site_id_b_4)).alias("kwh_change"),
        (F.col("voltage_plus_kwh") - F.lag("voltage_plus_kwh").over(w_read_at_dense_rank_distinct)).alias("voltage_plus_kwh_change"),
        F.nth_value("prev_voltage", 2).over(w_quality_by_last_known_site_c).alias("second_prev_voltage"),
    )
)

result = (
    ranked
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "quality_or_unknown",
        "site_commissioned_on",
        "site_commissioned_on_tercile",
        "voltage_vs_site_commissioned_on_month_quality_or",
        "prev_kwh",
        "voltage_plus_kwh_decile",
        "prev3_voltage_plus_kwh",
        "last_known_site_commissioned_on",
        "read_at_dense_rank",
        "prev_voltage",
        "voltage_plus_kwh_rank",
        "kwh_percent_rank",
        "voltage_plus_kwh_change",
        "second_prev_voltage",
    )
)
