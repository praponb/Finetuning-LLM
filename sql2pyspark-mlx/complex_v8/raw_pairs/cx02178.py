from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_or_unknown_by_kwh = Window.partitionBy("quality_or_unknown").orderBy(F.col("kwh").desc(), "id")
w_quality_by_read_at_full = Window.partitionBy("quality").orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_or_unknown_by_read_a = Window.partitionBy("quality_or_unknown").orderBy("read_at", "id")
w_read_at_to_date_quality_or_u = Window.partitionBy(F.to_date("read_at"), "quality_or_unknown")
w_quality_or_unknown_quality = Window.partitionBy("quality_or_unknown", "quality")
w_quality_or_unknown_by_kwh_ru = Window.partitionBy("quality_or_unknown").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_read_at = Window.partitionBy("quality").orderBy(F.col("read_at").desc_nulls_first(), "id")
w_quality_or_unknown_by_read_a_2 = Window.partitionBy("quality_or_unknown").orderBy(F.col("read_at").asc_nulls_last())
w_quality_by_read_at_ties = Window.partitionBy("quality").orderBy(F.col("read_at").desc_nulls_first())

prepared = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(2, 3)) & (F.col("site_id") >= 6))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

with_windows = (
    prepared
    .select(
        "id",
        "site_id",
        "voltage",
        "read_at_label",
        F.lag("read_at", 3).over(w_quality_or_unknown_by_kwh).alias("prev3_read_at"),
        F.first("voltage").over(w_quality_by_read_at_full).alias("first_voltage"),
        (F.col("voltage") - F.lag("voltage").over(w_quality_or_unknown_by_read_a)).alias("voltage_change"),
        F.sum("kwh").over(w_read_at_to_date_quality_or_u).alias("read_at_to_date_quality_or_unknown_sum_kwh"),
        F.count(F.lit(1)).over(w_quality_or_unknown_quality).alias("rows_in_quality_or_unknown_quality"),
        F.min("voltage").over(w_quality_or_unknown_by_kwh_ru).alias("min_voltage_so_far"),
        F.max("kwh").over(w_quality_or_unknown_quality).alias("quality_or_unknown_quality_max_kwh"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_quality_by_read_at))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.avg("kwh").over(w_quality_or_unknown_quality), 2).alias("quality_or_unknown_quality_avg_kwh"),
        F.round(F.cume_dist().over(w_quality_or_unknown_by_read_a_2), 3).alias("read_at_cume_dist"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_or_unknown_quality), F.lit(0)), 4).alias("kwh_share_of_quality_or_unknown_quality"),
        F.dense_rank().over(w_quality_by_read_at_ties).alias("read_at_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "voltage",
        "read_at_label",
        "rows_in_quality_or_unknown_quality",
        "min_voltage_so_far",
        "quality_or_unknown_quality_avg_kwh",
        "kwh_share_of_quality_or_unknown_quality",
        "read_at_dense_rank",
        F.least(F.col("read_at_dense_rank"), F.col("voltage")).alias("least_read_at_dense_rank_voltage"),
    )
    .orderBy(
        F.col("rows_in_quality_or_unknown_quality").desc(),
        "kwh_share_of_quality_or_unknown_quality",
    )
)
