from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_site_id_read_at_quarter = Window.partitionBy("site_id", "read_at_quarter")
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_read_at_year_site_id_by_qual = Window.partitionBy(F.year("read_at"), "site_id").orderBy("quality_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_zone_by_kwh = Window.partitionBy("site_zone").orderBy(F.col("kwh").desc(), "id")
w_read_at_year_site_id_by_qual_2 = Window.partitionBy(F.year("read_at"), "site_id").orderBy("quality_len")
w_read_at_year_site_id_by_qual_3 = Window.partitionBy(F.year("read_at"), "site_id").orderBy("quality_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_len_by_voltage_ties = Window.partitionBy("quality_len").orderBy("voltage")
w_read_at_year_site_id_by_qual_4 = Window.partitionBy(F.year("read_at"), "site_id").orderBy("quality_len", "id")
w_all_by_rows_in_site_id_read = Window.orderBy("rows_in_site_id_read_at_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_kwh_rank = Window.partitionBy("kwh_rank")
w_all_by_kwh_rank = Window.orderBy("kwh_rank", "id")
w_quality_len_rank_by_rows_in = Window.partitionBy("quality_len_rank").orderBy("rows_in_site_id_read_at_quarter", "id")
w_quality_len_rank_by_rows_in_2 = Window.partitionBy("quality_len_rank").orderBy("rows_in_site_id_read_at_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_rows_in_site_id_read_2 = Window.orderBy("rows_in_site_id_read_at_quarter", "id")

cleaned = (
    df_meter_readings
    .filter((~F.col("quality").isin("EST", "BAD")) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") <= 373.3)
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.read_at_quarter",
        "s.quality_len",
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

windowed = (
    joined
    .select(
        "id",
        "site_id",
        "kwh",
        "quality_len",
        "site_zone",
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.count(F.lit(1)).over(w_site_id_read_at_quarter).alias("rows_in_site_id_read_at_quarter"),
        F.lead("read_at").over(w_all_by_kwh).alias("next_read_at"),
        F.nth_value("quality_len", 2).over(w_read_at_year_site_id_by_qual).alias("second_quality_len"),
        F.rank().over(w_site_zone_by_kwh).alias("kwh_rank"),
        F.rank().over(w_read_at_year_site_id_by_qual_2).alias("quality_len_rank"),
        F.sum("voltage").over(w_read_at_year_site_id_by_qual_3).alias("running_voltage"),
        F.round(F.cume_dist().over(w_quality_len_by_voltage_ties), 3).alias("voltage_cume_dist"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.col("read_at_quarter") - F.avg("read_at_quarter").over(w_site_id), 2).alias("read_at_quarter_vs_site_id_avg"),
        F.size(F.collect_set("site_zone").over(w_site_id)).alias("distinct_site_zone_in_site_id"),
        F.dense_rank().over(w_read_at_year_site_id_by_qual_4).alias("quality_len_dense_rank"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "site_id",
        "site_zone",
        "kwh_vs_site_id_avg",
        "rows_in_site_id_read_at_quarter",
        "next_read_at",
        "second_quality_len",
        "kwh_rank",
        "running_voltage",
        "voltage_cume_dist",
        "rows_in_site_id",
        "read_at_quarter_vs_site_id_avg",
        "distinct_site_zone_in_site_id",
        "quality_len_dense_rank",
        F.first("kwh_rank").over(w_all_by_rows_in_site_id_read).alias("first_kwh_rank"),
        F.round(F.col("quality_len_rank") - F.avg("quality_len_rank").over(w_kwh_rank), 2).alias("quality_len_rank_vs_kwh_rank_avg"),
        F.lag("quality_len").over(w_all_by_kwh_rank).alias("prev_quality_len"),
        F.round(F.col("quality_len") - F.avg("quality_len").over(w_kwh_rank), 2).alias("quality_len_vs_kwh_rank_avg"),
        F.lead("rows_in_site_id").over(w_quality_len_rank_by_rows_in).alias("next_rows_in_site_id"),
        F.sum("quality_len").over(w_quality_len_rank_by_rows_in_2).alias("running_quality_len"),
        F.lag("next_read_at").over(w_all_by_rows_in_site_id_read_2).alias("prev_next_read_at"),
    )
)

result = (
    with_windows
    .filter(F.col("running_quality_len") < 106)
    .select(
        "id",
        "site_id",
        "kwh_vs_site_id_avg",
        "rows_in_site_id_read_at_quarter",
        "next_read_at",
        "running_voltage",
        "voltage_cume_dist",
        "rows_in_site_id",
        "read_at_quarter_vs_site_id_avg",
        "first_kwh_rank",
        "quality_len_vs_kwh_rank_avg",
        "next_rows_in_site_id",
        "running_quality_len",
    )
    .orderBy(F.col("running_quality_len").asc_nulls_last())
)
