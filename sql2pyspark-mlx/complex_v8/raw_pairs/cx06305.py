from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy("voltage", "id")
w_all_by_quality_len_ties = Window.orderBy(F.col("quality_len").desc())
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy(F.col("read_at").asc_nulls_last(), "id")
w_quality_by_read_at = Window.partitionBy("quality").orderBy(F.col("read_at").desc(), "id")
w_read_at_to_date = Window.partitionBy(F.to_date("read_at"))
w_site_id_by_voltage_full = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_quality_len_by_site = Window.partitionBy("quality", "quality_len").orderBy("site_installed_kw")
w_quality_len_rank_by_prev_kwh = Window.partitionBy("quality_len_rank").orderBy(F.col("prev_kwh").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_quality_len_rank_by_prev_kwh_2 = Window.partitionBy("quality_len_rank").orderBy(F.col("prev_kwh").desc_nulls_first(), "id")
w_site_id_quality_len_rank = Window.partitionBy("site_id", "quality_len_rank")
w_quality = Window.partitionBy("quality")

valid_readings = (
    df_meter_readings
    .filter((F.col("kwh") >= 35.9) & (F.col("voltage") <= 232.7))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
    )
)

with_parent = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.site_name").isNotNull()), "inner")
    .select("s.*", F.col("si.installed_kw").alias("site_installed_kw"))
)

with_trends = (
    with_parent
    .select(
        "*",
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
        F.lag("quality_len", 3).over(w_site_id_by_voltage).alias("prev3_quality_len"),
        F.rank().over(w_all_by_quality_len_ties).alias("quality_len_rank"),
        F.row_number().over(w_site_id_by_read_at).alias("read_at_seq"),
        F.row_number().over(w_quality_by_read_at).alias("seq_no"),
        F.count(F.lit(1)).over(w_read_at_to_date).alias("rows_in_read_at_to_date"),
        F.nth_value("quality_len", 2).over(w_site_id_by_voltage_full).alias("second_quality_len"),
        F.nth_value("kwh", 2).over(w_site_id_by_voltage_full).alias("second_kwh"),
        F.dense_rank().over(w_quality_quality_len_by_site).alias("site_installed_kw_dense_rank"),
        F.min("kwh").over(w_quality_site_id).alias("quality_site_id_min_kwh"),
        F.lag("kwh").over(w_quality_by_read_at).alias("prev_kwh"),
    )
)

top_rows = (
    with_trends
    .filter((F.col("seq_no") <= 5) & (F.col("quality_len_rank") < 8))
    .drop("seq_no")
)

ranked = (
    top_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "quality_len",
        "read_at_label",
        "prev3_quality_len",
        "quality_len_rank",
        "rows_in_read_at_to_date",
        "second_quality_len",
        "second_kwh",
        "site_installed_kw_dense_rank",
        "quality_site_id_min_kwh",
        "prev_kwh",
        F.round(F.avg("quality_site_id_min_kwh").over(w_quality_len_rank_by_prev_kwh), 2).alias("quality_site_id_min_kwh_avg_3_prior"),
        F.lead("quality_len", 1, 0).over(w_quality_len_rank_by_prev_kwh_2).alias("next_quality_len"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id_quality_len_rank), 2).alias("voltage_vs_site_id_quality_len_rank_avg"),
        F.round(F.col("read_at_seq") - F.avg("read_at_seq").over(w_quality), 2).alias("read_at_seq_vs_quality_avg"),
    )
)

result = (
    ranked
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_len",
        "rows_in_read_at_to_date",
        "second_quality_len",
        "second_kwh",
        "site_installed_kw_dense_rank",
        "prev_kwh",
        "next_quality_len",
        "voltage_vs_site_id_quality_len_rank_avg",
    )
    .orderBy(
        "voltage",
        F.col("voltage_vs_site_id_quality_len_rank_avg").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
