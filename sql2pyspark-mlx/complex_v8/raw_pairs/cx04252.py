from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_read_at_year_by_read = Window.partitionBy("site_id", F.year("read_at")).orderBy(F.col("read_at").desc(), "id")
w_site_id = Window.partitionBy("site_id")
w_all_by_kwh_ties = Window.orderBy(F.col("kwh").asc_nulls_last())
w_site_id_quality_by_read_at_m = Window.partitionBy("site_id", "quality").orderBy("read_at", "id").rowsBetween(-2, Window.currentRow)
w_site_id_quality_by_read_at_p = Window.partitionBy("site_id", "quality").orderBy("read_at", "id").rowsBetween(-3, -1)
w_site_id_quality_by_read_at_t = Window.partitionBy("site_id", "quality").orderBy("read_at")
w_kwh_dense_rank_quality_by_kw = Window.partitionBy("kwh_dense_rank", "quality").orderBy("kwh_percent_rank", "id")
w_kwh_dense_rank_quality_by_kw_2 = Window.partitionBy("kwh_dense_rank", "quality").orderBy("kwh_percent_rank")
w_kwh_dense_rank_quality_by_kw_3 = Window.partitionBy("kwh_dense_rank", "quality").orderBy("kwh_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_readings = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(1, 3)) & (F.col("quality").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

metrics = (
    recent_readings
    .select(
        "*",
        F.lead("voltage", 3, 0).over(w_site_id_read_at_year_by_read).alias("next3_voltage"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.dense_rank().over(w_all_by_kwh_ties).alias("kwh_dense_rank"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.rank().over(w_all_by_kwh_ties).alias("kwh_rank"),
        F.max("voltage").over(w_site_id_quality_by_read_at_m).alias("voltage_max_3"),
        F.round(F.percent_rank().over(w_all_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.max("kwh").over(w_site_id_quality_by_read_at_p).alias("kwh_max_3_prior"),
        F.round(F.avg("kwh").over(w_site_id), 2).alias("site_id_avg_kwh"),
        F.rank().over(w_site_id_quality_by_read_at_t).alias("read_at_rank"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "next3_voltage",
        "kwh_dense_rank",
        "kwh_percent_rank",
        "site_id_avg_kwh",
        "read_at_rank",
        (F.col("kwh_rank") - F.col("rows_in_site_id")).alias("kwh_rank_minus_rows_in_site_id"),
        F.round(F.col("kwh_rank") / F.nullif(F.col("next3_voltage"), F.lit(0)), 3).alias("kwh_rank_per_next3_voltage"),
        F.hour(F.col("read_at")).alias("read_at_hour"),
    )
)

metrics_2 = (
    enriched
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "kwh_dense_rank",
        "kwh_percent_rank",
        "site_id_avg_kwh",
        "read_at_rank",
        "kwh_rank_per_next3_voltage",
        "read_at_hour",
        F.lead("next3_voltage").over(w_kwh_dense_rank_quality_by_kw).alias("next_next3_voltage"),
        F.round(F.cume_dist().over(w_kwh_dense_rank_quality_by_kw_2), 3).alias("kwh_percent_rank_cume_dist"),
        F.last("site_id_avg_kwh").over(w_kwh_dense_rank_quality_by_kw_3).alias("last_site_id_avg_kwh"),
    )
)

result = (
    metrics_2
    .filter(F.col("kwh_percent_rank") <= 0.8)
    .select(
        "id",
        "site_id",
        "kwh",
        "kwh_dense_rank",
        "kwh_percent_rank",
        "site_id_avg_kwh",
        "read_at_rank",
        "kwh_rank_per_next3_voltage",
        "read_at_hour",
        "next_next3_voltage",
        "kwh_percent_rank_cume_dist",
        "last_site_id_avg_kwh",
        F.round(F.col("kwh_dense_rank") / F.nullif(F.col("last_site_id_avg_kwh"), F.lit(0)), 3).alias("kwh_dense_rank_per_last_site_id_avg_kwh"),
        F.least(F.col("next_next3_voltage"), F.col("voltage")).alias("least_next_next3_voltage_voltage"),
    )
    .orderBy(F.col("next_next3_voltage").desc())
)
