from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_ties = Window.orderBy(F.col("read_at").asc_nulls_last())
w_all_by_read_at_full = Window.orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_read_at = Window.orderBy(F.col("read_at").asc_nulls_last(), "id")
w_all_by_voltage_prior_2 = Window.orderBy("voltage", "id").rowsBetween(-2, -1)
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_voltage = Window.orderBy("voltage", "id")
w_voltage_quartile_second_read = Window.partitionBy("voltage_quartile", F.date_trunc("month", "second_read_at"))
w_all_by_second_read_at_ties = Window.orderBy("second_read_at")
w_all_by_second_read_at = Window.orderBy("second_read_at", "id")
w_second_read_at_dense_rank = Window.partitionBy("second_read_at_dense_rank")
w_all_by_voltage_vs_voltage_qu = Window.orderBy(F.col("voltage_vs_voltage_quartile_second_read_at_month").desc(), "id").rowsBetween(-6, -1)

cleaned_readings = (
    df_meter_readings
    .filter((F.col("kwh") >= 34.6) & (F.col("voltage") >= 218.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 1.1, 2).alias("kwh_adj"),
    )
)

sequenced = (
    cleaned_readings
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "kwh_adj",
        F.round(F.percent_rank().over(w_all_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.nth_value("read_at", 2).over(w_all_by_read_at_full).alias("second_read_at"),
        F.lag("voltage", 3, 0).over(w_all_by_read_at).alias("prev3_voltage"),
        F.sum("voltage").over(w_all_by_voltage_prior_2).alias("voltage_sum_2_prior"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
        F.ntile(4).over(w_all_by_voltage).alias("voltage_quartile"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.round(F.col("voltage_sum_2_prior") / F.nullif(F.sum("voltage_sum_2_prior").over(w_voltage_quartile_second_read), F.lit(0)), 4).alias("voltage_sum_2_prior_share_of_voltage_quartile_se"),
        F.dense_rank().over(w_all_by_second_read_at_ties).alias("second_read_at_dense_rank"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_voltage_quartile_second_read), 2).alias("voltage_vs_voltage_quartile_second_read_at_month"),
        F.lead("read_at_percent_rank", 2, 0).over(w_all_by_second_read_at).alias("next2_read_at_percent_rank"),
    )
)

enriched = (
    sequenced_2
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-02-18").cast("date"))
    .select(
        "*",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.round(F.col("voltage_sum_2_prior") * 0.9, 2).alias("voltage_sum_2_prior_adj"),
    )
)

sequenced_3 = (
    enriched
    .select(
        "*",
        F.count(F.lit(1)).over(w_second_read_at_dense_rank).alias("rows_in_second_read_at_dense_rank"),
        F.size(F.collect_set("quality").over(w_second_read_at_dense_rank)).alias("distinct_quality_in_second_read_at_dense_rank"),
        F.round(F.avg("second_read_at_dense_rank").over(w_all_by_voltage_vs_voltage_qu), 2).alias("second_read_at_dense_rank_avg_6_prior"),
    )
)

result = (
    sequenced_3
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "read_at_percent_rank",
        "second_read_at",
        "voltage_sum_2_prior",
        "voltage_quartile",
        "voltage_sum_2_prior_share_of_voltage_quartile_se",
        "second_read_at_dense_rank",
        "voltage_vs_voltage_quartile_second_read_at_month",
        "next2_read_at_percent_rank",
        "quality_prefix",
        "voltage_sum_2_prior_adj",
        "distinct_quality_in_second_read_at_dense_rank",
        "second_read_at_dense_rank_avg_6_prior",
        F.date_format(F.col("second_read_at"), "yyyy-MM").alias("second_read_at_label"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
    .orderBy(
        F.col("voltage_vs_voltage_quartile_second_read_at_month").desc(),
        F.col("quality_prefix").desc_nulls_first(),
    )
)
