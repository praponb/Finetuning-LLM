from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_quality_len_read_at_plus_30m = Window.partitionBy("quality_len", F.month("read_at_plus_30m"))
w_quality_len_site_id_by_volta = Window.partitionBy("quality_len", "site_id").orderBy("voltage", "id")
w_all_by_read_at_plus_30m_runn = Window.orderBy("read_at_plus_30m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_len_site_id_by_volta_2 = Window.partitionBy("quality_len", "site_id").orderBy("voltage")
w_site_id_quality_len_by_read = Window.partitionBy("site_id", "quality_len").orderBy(F.col("read_at_plus_30m").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_all_by_read_at_plus_30m_full = Window.orderBy(F.col("read_at_plus_30m").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_read_at_plus_30m_runn_2 = Window.orderBy(F.col("read_at_plus_30m").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_len_site_id_by_volta_3 = Window.partitionBy("quality_len", "site_id").orderBy("voltage", "id").rowsBetween(-2, -1)
w_read_at_plus_30m_month = Window.partitionBy(F.date_trunc("month", "read_at_plus_30m"))

prepared = (
    df_meter_readings
    .filter(F.col("voltage").between(219.5, 233.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        (F.col("read_at") + F.expr("INTERVAL 30 MINUTES")).alias("read_at_plus_30m"),
    )
)

with_trends = (
    prepared
    .select(
        "*",
        F.round(F.col("quality_len") / F.nullif(F.sum("quality_len").over(w_quality), F.lit(0)), 4).alias("quality_len_share_of_quality"),
        F.round(F.avg("quality_len").over(w_quality_len_read_at_plus_30m), 2).alias("quality_len_read_at_plus_30m_month_avg_quality_l"),
        F.sum("kwh").over(w_quality).alias("quality_sum_kwh"),
        F.lead("read_at_plus_30m").over(w_quality_len_site_id_by_volta).alias("next_read_at_plus_30m"),
        F.sum("quality_len").over(w_all_by_read_at_plus_30m_runn).alias("running_quality_len"),
        F.round(F.cume_dist().over(w_quality_len_site_id_by_volta_2), 3).alias("voltage_cume_dist"),
        F.max("kwh").over(w_site_id_quality_len_by_read).alias("kwh_max_2_prior"),
        F.last("kwh").over(w_all_by_read_at_plus_30m_full).alias("last_kwh"),
        F.count(F.lit(1)).over(w_quality_len_read_at_plus_30m).alias("rows_in_quality_len_read_at_plus_30m_month"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.first("kwh", ignorenulls=True).over(w_all_by_read_at_plus_30m_runn_2).alias("first_known_kwh"),
        F.rank().over(w_quality_len_site_id_by_volta).alias("voltage_rank"),
        F.max("kwh").over(w_quality_len_site_id_by_volta_3).alias("kwh_max_2_prior_2"),
        F.row_number().over(w_quality_len_site_id_by_volta).alias("voltage_seq"),
        F.round(F.col("quality_len") / F.nullif(F.sum("quality_len").over(w_quality_len_read_at_plus_30m), F.lit(0)), 4).alias("quality_len_share_of_quality_len_read_at_plus_30"),
        F.count(F.lit(1)).over(w_read_at_plus_30m_month).alias("rows_in_read_at_plus_30m_month"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_len",
        "read_at_plus_30m",
        "quality_len_share_of_quality",
        "quality_sum_kwh",
        "next_read_at_plus_30m",
        "running_quality_len",
        "kwh_max_2_prior",
        "last_kwh",
        "rows_in_quality_len_read_at_plus_30m_month",
        "rows_in_quality",
        "first_known_kwh",
        "voltage_rank",
        "kwh_max_2_prior_2",
        "rows_in_read_at_plus_30m_month",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
    .orderBy("kwh_max_2_prior", F.col("read_at_plus_30m").desc_nulls_first())
)
