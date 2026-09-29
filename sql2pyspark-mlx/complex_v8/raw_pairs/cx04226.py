from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff_quality_or_unkno = Window.partitionBy("site_tariff", "quality_or_unknown").orderBy("site_commissioned_on", "id")
w_quality = Window.partitionBy("quality")
w_site_tariff_quality_or_unkno_2 = Window.partitionBy("site_tariff", "quality_or_unknown").orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_quality_or_unknown_b = Window.partitionBy("site_id", "quality_or_unknown").orderBy(F.col("voltage").desc(), "id")
w_quality_quality_or_unknown_b = Window.partitionBy("quality", "quality_or_unknown").orderBy("kwh", "id")
w_site_id_by_site_installed_kw = Window.partitionBy("site_id").orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_seq_no_quality = Window.partitionBy("seq_no", "quality")
w_rn_read_at_to_date_by_seq_no = Window.partitionBy("rn", F.to_date("read_at")).orderBy("seq_no", "id")
w_site_tariff_by_site_installe = Window.partitionBy("site_tariff").orderBy(F.col("site_installed_kw_share_of_quality").desc())
w_site_tariff_by_site_installe_2 = Window.partitionBy("site_tariff").orderBy(F.col("site_installed_kw_share_of_quality").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_tariff_by_site_installe_3 = Window.partitionBy("site_tariff").orderBy(F.col("site_installed_kw_share_of_quality").desc(), "id")

filtered = (
    df_meter_readings
    .filter((F.col("voltage").between(221.8, 240.2)) & (F.col("quality") == "EST"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

with_parent_site = (
    filtered.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") > 96.9), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_or_unknown",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

sequenced = (
    with_parent_site
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "quality_or_unknown",
        "site_tariff",
        "site_installed_kw",
        "site_commissioned_on",
        F.row_number().over(w_site_tariff_quality_or_unkno).alias("rn"),
        F.round(F.col("site_installed_kw") - F.avg("site_installed_kw").over(w_quality), 2).alias("site_installed_kw_vs_quality_avg"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_quality), F.lit(0)), 4).alias("site_installed_kw_share_of_quality"),
        F.nth_value("voltage", 2).over(w_site_tariff_quality_or_unkno_2).alias("second_voltage"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality), F.lit(0)), 4).alias("voltage_share_of_quality"),
        F.row_number().over(w_site_id_quality_or_unknown_b).alias("seq_no"),
        F.row_number().over(w_quality_quality_or_unknown_b).alias("kwh_seq"),
        F.first("site_installed_kw").over(w_site_id_by_site_installed_kw).alias("first_site_installed_kw"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.count(F.lit(1)).over(w_seq_no_quality).alias("rows_in_seq_no_quality"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_rn_read_at_to_date_by_seq_no))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.lead("rn", 1, 0).over(w_rn_read_at_to_date_by_seq_no).alias("next_rn"),
        F.min("voltage_share_of_quality").over(w_seq_no_quality).alias("seq_no_quality_min_voltage_share_of_quality"),
        F.dense_rank().over(w_site_tariff_by_site_installe).alias("site_installed_kw_share_of_quality_dense_rank"),
        F.max("site_installed_kw").over(w_site_tariff_by_site_installe_2).alias("max_site_installed_kw_so_far"),
        F.lead("rn", 2).over(w_site_tariff_by_site_installe_3).alias("next2_rn"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "site_id",
        "read_at",
        "quality_or_unknown",
        "site_tariff",
        "site_commissioned_on",
        "site_installed_kw_vs_quality_avg",
        "site_installed_kw_share_of_quality",
        "second_voltage",
        "voltage_share_of_quality",
        "seq_no",
        "kwh_seq",
        "rows_in_quality",
        "rows_in_seq_no_quality",
        "minutes_since_prev_read_at",
        "next_rn",
        "site_installed_kw_share_of_quality_dense_rank",
        "max_site_installed_kw_so_far",
        "next2_rn",
        F.length(F.trim(F.col("quality_or_unknown"))).alias("quality_or_unknown_len"),
    )
    .orderBy(F.col("voltage_share_of_quality").desc())
)
