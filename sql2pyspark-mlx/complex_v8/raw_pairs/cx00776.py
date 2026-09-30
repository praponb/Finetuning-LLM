from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_kwh_bucket = Window.partitionBy("site_id").orderBy("kwh_bucket", "id")
w_read_at_year = Window.partitionBy(F.year("read_at"))
w_all_by_voltage = Window.orderBy("voltage", "id")
w_site_id_by_kwh_bucket_prior = Window.partitionBy("site_id").orderBy("kwh_bucket", "id").rowsBetween(-1, -1)
w_all_by_prev_kwh_full = Window.orderBy(F.col("prev_kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rn_read_at_to_date = Window.partitionBy("rn", F.to_date("read_at"))
w_rn_by_distinct_quality_in_re = Window.partitionBy("rn").orderBy(F.col("distinct_quality_in_read_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_meter_readings
    .filter((F.col("kwh") > 35.0) & (F.col("quality").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

with_trends = (
    recent
    .select(
        "*",
        F.lag("kwh").over(w_site_id_by_kwh_bucket).alias("prev_kwh"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_year), F.lit(0)), 4).alias("kwh_share_of_read_at_year"),
        F.row_number().over(w_all_by_voltage).alias("rn"),
        F.size(F.collect_set("quality").over(w_read_at_year)).alias("distinct_quality_in_read_at_year"),
        F.round(F.avg("voltage").over(w_site_id_by_kwh_bucket_prior), 2).alias("voltage_avg_1_prior"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "prev_kwh",
        "kwh_share_of_read_at_year",
        "rn",
        "distinct_quality_in_read_at_year",
        F.last("kwh").over(w_all_by_prev_kwh_full).alias("last_kwh"),
        F.count(F.lit(1)).over(w_rn_read_at_to_date).alias("rows_in_rn_read_at_to_date"),
        F.sum("kwh_share_of_read_at_year").over(w_rn_read_at_to_date).alias("rn_read_at_to_date_sum_kwh_share_of_read_at_year"),
        F.round(F.col("voltage_avg_1_prior") - F.avg("voltage_avg_1_prior").over(w_rn_read_at_to_date), 2).alias("voltage_avg_1_prior_vs_rn_read_at_to_date_avg"),
        F.first("kwh").over(w_rn_by_distinct_quality_in_re).alias("first_kwh"),
    )
)

per_group = (
    metrics
    .filter(F.col("rn") <= 7)
    .groupBy("quality", "site_id", "distinct_quality_in_read_at_year")
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.round(F.avg("voltage_avg_1_prior_vs_rn_read_at_to_date_avg"), 2).alias("avg_voltage_avg_1_prior_vs_rn_read_at_to_date_av"),
        F.sum(F.when(~F.col("quality").isin("OK", "BAD"), 1).otherwise(0)).alias("n_quality_not_in_ok_bad"),
        F.round(F.avg("prev_kwh"), 2).alias("avg_prev_kwh"),
    )
)

joined_site = (
    per_group.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.quality",
        "s.site_id",
        "s.distinct_quality_in_read_at_year",
        "s.quality_list",
        "s.avg_voltage_avg_1_prior_vs_rn_read_at_to_date_av",
        "s.n_quality_not_in_ok_bad",
        "s.avg_prev_kwh",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

combined_details_site = (
    joined_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.quality",
        "s.site_id",
        "s.distinct_quality_in_read_at_year",
        "s.quality_list",
        "s.avg_voltage_avg_1_prior_vs_rn_read_at_to_date_av",
        "s.n_quality_not_in_ok_bad",
        "s.site_installed_kw",
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    combined_details_site
    .cube("n_quality_not_in_ok_bad", "distinct_quality_in_read_at_year")
    .agg(F.array_sort(F.collect_set("quality")).alias("quality_list_2"))
    .orderBy(
        F.col("n_quality_not_in_ok_bad").asc_nulls_last(),
        F.col("distinct_quality_in_read_at_year").asc_nulls_last(),
    )
)
