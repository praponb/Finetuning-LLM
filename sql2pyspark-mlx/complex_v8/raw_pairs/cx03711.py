from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_kwh = Window.partitionBy("quality").orderBy(F.col("kwh").asc_nulls_last(), "id")
w_site_tariff_site_id = Window.partitionBy("site_tariff", "site_id")
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh_full = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_all_by_kwh_ties = Window.orderBy(F.col("kwh").desc())
w_site_id_by_voltage_adj = Window.partitionBy("site_id").orderBy(F.col("voltage_adj").asc_nulls_last(), "id")

recent = (
    df_meter_readings
    .filter((F.col("voltage").between(218.2, 239.6)) | (F.col("kwh") < 82.9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") * 1.07, 2).alias("voltage_adj"),
    )
)

combined_details = (
    recent.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on") < F.lit("2025-11-28").cast("date")), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.voltage_adj",
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
    )
)

metrics = (
    combined_details
    .select(
        "*",
        F.round(F.percent_rank().over(w_quality_by_kwh), 3).alias("kwh_percent_rank"),
        F.size(F.collect_set("quality").over(w_site_tariff_site_id)).alias("distinct_quality_in_site_tariff_site_id"),
        F.count(F.lit(1)).over(w_site_tariff_site_id).alias("rows_in_site_tariff_site_id"),
        F.first("read_at", ignorenulls=True).over(w_all_by_kwh_running).alias("first_known_read_at"),
        F.last("kwh").over(w_all_by_kwh_full).alias("last_kwh"),
        F.round(F.avg("voltage_adj").over(w_site_tariff_site_id), 2).alias("site_tariff_site_id_avg_voltage_adj"),
        F.lag("read_at", 2).over(w_all_by_kwh).alias("prev2_read_at"),
        F.dense_rank().over(w_all_by_kwh_ties).alias("kwh_dense_rank"),
        F.ntile(10).over(w_site_id_by_voltage_adj).alias("voltage_adj_decile"),
        F.rank().over(w_quality_by_kwh).alias("kwh_rank"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_kwh))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.lead("read_at").over(w_all_by_kwh).alias("next_read_at"),
    )
)

summary = (
    metrics
    .groupBy(F.to_date("next_read_at").alias("next_read_at_day"))
    .agg(
        F.round(F.avg("distinct_quality_in_site_tariff_site_id"), 2).alias("avg_distinct_quality_in_site_tariff_site_id"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.array_sort(F.collect_set("site_tariff")).alias("site_tariff_list"),
        F.max("kwh_rank").alias("max_kwh_rank"),
        F.sum("minutes_since_prev_read_at").alias("total_minutes_since_prev_read_at"),
    )
)

result = (
    summary
    .filter(F.size(F.col("site_tariff_list")) >= 1)
    .select(
        "next_read_at_day",
        "avg_distinct_quality_in_site_tariff_site_id",
        "site_tariff_list",
        "max_kwh_rank",
        "total_minutes_since_prev_read_at",
        (F.floor(F.col("distinct_site_ids") / 2) * 2).alias("distinct_site_ids_bucket"),
    )
    .orderBy(
        F.col("max_kwh_rank").asc_nulls_last(),
        F.col("total_minutes_since_prev_read_at").desc(),
    )
)
