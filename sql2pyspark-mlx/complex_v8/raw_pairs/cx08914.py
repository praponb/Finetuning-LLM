from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_by_kwh_prior_3 = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("kwh").desc(), "id").rowsBetween(-3, -1)
w_all_by_kwh_moving_6 = Window.orderBy("kwh", "id").rowsBetween(-6, Window.currentRow)
w_site_id_quality_by_kwh = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").desc(), "id")
w_quality = Window.partitionBy("quality")
w_all_by_kwh_ties = Window.orderBy("kwh")
w_read_at_month_by_kwh_full = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh = Window.orderBy("kwh", "id")
w_quality_by_read_at = Window.partitionBy("quality").orderBy("read_at", "id")
w_all_by_n_rows_moving_3 = Window.orderBy("n_rows", "quality").rowsBetween(-3, Window.currentRow)
w_distinct_ids_n_rows = Window.partitionBy("distinct_ids", "n_rows")
w_all_by_n_rows = Window.orderBy("n_rows", "quality")

base = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

combined_details = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.site_name").isNotNull())
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_slug",
        "si.site_name",
    )
)

with_trends = (
    combined_details
    .select(
        "*",
        F.sum("voltage").over(w_read_at_month_by_kwh_prior_3).alias("voltage_sum_3_prior"),
        F.round(F.avg("kwh").over(w_all_by_kwh_moving_6), 2).alias("kwh_avg_7"),
        F.lag("read_at", 2).over(w_site_id_quality_by_kwh).alias("prev2_read_at"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality), 2).alias("kwh_vs_quality_avg"),
        F.lag("voltage", 3).over(w_site_id_quality_by_kwh).alias("prev3_voltage"),
        F.round(F.percent_rank().over(w_all_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.last("kwh").over(w_read_at_month_by_kwh_full).alias("last_kwh"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.lead("voltage").over(w_all_by_kwh).alias("next_voltage"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_kwh))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.percent_rank().over(w_quality_by_read_at), 3).alias("read_at_percent_rank"),
        F.lead("voltage", 3).over(w_all_by_kwh).alias("next3_voltage"),
    )
)

stats = (
    with_trends
    .groupBy("quality")
    .agg(
        F.array_sort(F.collect_set("site_name")).alias("site_name_list"),
        F.max("kwh_percent_rank").alias("max_kwh_percent_rank"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("distinct_site_id_in_quality"), 2).alias("stddev_distinct_site_id_in_quality"),
        F.count("*").alias("n_rows"),
    )
)

windowed = (
    stats
    .select(
        "*",
        F.round(F.avg("n_rows").over(w_all_by_n_rows_moving_3), 2).alias("n_rows_avg_4"),
        F.min("distinct_ids").over(w_distinct_ids_n_rows).alias("distinct_ids_n_rows_min_distinct_ids"),
        F.rank().over(w_all_by_n_rows).alias("n_rows_rank"),
    )
)

result = (
    windowed
    .select("quality", "n_rows", "distinct_ids_n_rows_min_distinct_ids")
    .distinct()
    .orderBy(F.col("quality").desc_nulls_first(), F.col("n_rows").desc())
)
