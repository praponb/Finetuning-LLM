from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_voltage_adj_movin = Window.partitionBy("quality").orderBy("voltage_adj", "id").rowsBetween(-2, Window.currentRow)
w_read_at_month_by_voltage_adj = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("voltage_adj").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_kwh_ties = Window.orderBy("kwh")
w_quality_read_at_year = Window.partitionBy("quality", F.year("read_at"))
w_all_by_read_at_full = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh_full = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month_by_voltage_adj_2 = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("voltage_adj").desc(), "id").rowsBetween(-6, -1)
w_all_by_kwh_share_of_quality = Window.orderBy("kwh_share_of_quality_read_at_year")
w_site_id = Window.partitionBy("site_id")
w_all_by_kwh_share_of_quality_2 = Window.orderBy("kwh_share_of_quality_read_at_year", "id")

base = (
    df_meter_readings
    .filter((F.col("quality").isNotNull()) & (F.col("read_at") < F.lit("2025-02-18 13:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") * 1.1, 2).alias("voltage_adj"),
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.round(F.avg("kwh").over(w_quality_by_voltage_adj_movin), 2).alias("kwh_avg_3"),
        F.round(F.avg("voltage_adj").over(w_read_at_month_by_voltage_adj), 2).alias("voltage_adj_avg_3"),
        F.round(F.percent_rank().over(w_all_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_read_at_year), F.lit(0)), 4).alias("kwh_share_of_quality_read_at_year"),
        F.first("voltage_adj").over(w_all_by_read_at_full).alias("first_voltage_adj"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality_read_at_year), 2).alias("kwh_vs_quality_read_at_year_avg"),
        F.last("voltage_adj").over(w_all_by_kwh_full).alias("last_voltage_adj"),
        F.max("voltage_adj").over(w_quality_read_at_year).alias("quality_read_at_year_max_voltage_adj"),
        F.round(F.avg("kwh").over(w_read_at_month_by_voltage_adj_2), 2).alias("kwh_avg_6_prior"),
    )
)

tagged = (
    metrics
    .filter(F.col("kwh_share_of_quality_read_at_year").between(0.3, 0.8))
    .select(
        "*",
        F.round(F.col("last_voltage_adj") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("last_voltage_adj_per_kwh"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        F.dense_rank().over(w_all_by_kwh_share_of_quality).alias("kwh_share_of_quality_read_at_year_dense_rank"),
        F.round(F.col("kwh_avg_6_prior") - F.avg("kwh_avg_6_prior").over(w_site_id), 2).alias("kwh_avg_6_prior_vs_site_id_avg"),
        F.round(F.col("voltage_plus_kwh") / F.nullif(F.sum("voltage_plus_kwh").over(w_site_id), F.lit(0)), 4).alias("voltage_plus_kwh_share_of_site_id"),
        F.row_number().over(w_all_by_kwh_share_of_quality_2).alias("rn"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
    )
)

result = (
    windowed
    .filter(F.col("kwh_vs_quality_read_at_year_avg") >= -60.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "voltage_plus_kwh",
        "kwh_percent_rank",
        "first_voltage_adj",
        "last_voltage_adj",
        "quality_read_at_year_max_voltage_adj",
        "kwh_avg_6_prior",
        "last_voltage_adj_per_kwh",
        "kwh_share_of_quality_read_at_year_dense_rank",
        "kwh_avg_6_prior_vs_site_id_avg",
        "voltage_plus_kwh_share_of_site_id",
        "rn",
        "rows_in_site_id",
    )
    .orderBy(
        F.col("quality_read_at_year_max_voltage_adj").asc_nulls_last(),
        "kwh_percent_rank",
        "id",
    )
    .limit(5)
)
