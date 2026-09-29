from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_by_read_at_m = Window.partitionBy("site_id", "quality").orderBy("read_at_month", "id")
w_read_at_year_site_id = Window.partitionBy(F.year("read_at"), "site_id")
w_quality_by_kwh = Window.partitionBy("quality").orderBy("kwh", "id")
w_site_id_quality_by_read_at_m_2 = Window.partitionBy("site_id", "quality").orderBy("read_at_month", "id").rowsBetween(-1, Window.currentRow)
w_quality_site_tariff_by_volta = Window.partitionBy("quality", "site_tariff").orderBy(F.col("voltage_minus_kwh").desc(), "id")
w_quality_site_id_by_voltage_m = Window.partitionBy("quality", "site_id").orderBy("voltage_minus_kwh", "id")
w_quality = Window.partitionBy("quality")
w_distinct_quality_in_read_at = Window.partitionBy("distinct_quality_in_read_at_year_site_id").orderBy(F.col("kwh_change").desc(), "id")
w_site_tariff_by_next2_read_at = Window.partitionBy("site_tariff").orderBy(F.col("next2_read_at").desc(), "id")
w_n_rows_by_n_rows_ties = Window.partitionBy("n_rows").orderBy(F.col("n_rows").desc())
w_n_rows = Window.partitionBy("n_rows")

valid_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
    )
)

with_details = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.read_at_month",
        "s.voltage_minus_kwh",
        F.col("si.tariff").alias("site_tariff"),
    )
)

sequenced = (
    with_details
    .select(
        "*",
        F.round((F.unix_timestamp("read_at_month") - F.unix_timestamp(F.lag("read_at_month").over(w_site_id_quality_by_read_at_m))) / 60, 1).alias("minutes_since_prev_read_at_month"),
        F.count(F.lit(1)).over(w_read_at_year_site_id).alias("rows_in_read_at_year_site_id"),
        F.lead("read_at", 2).over(w_site_id_quality_by_read_at_m).alias("next2_read_at"),
        F.round(F.percent_rank().over(w_quality_by_kwh), 3).alias("kwh_percent_rank"),
        F.size(F.collect_set("quality").over(w_read_at_year_site_id)).alias("distinct_quality_in_read_at_year_site_id"),
        F.max("voltage_minus_kwh").over(w_site_id_quality_by_read_at_m_2).alias("voltage_minus_kwh_max_2"),
        F.row_number().over(w_quality_site_tariff_by_volta).alias("rn"),
        (F.col("kwh") - F.lag("kwh").over(w_quality_site_id_by_voltage_m)).alias("kwh_change"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "site_id",
        "kwh",
        "read_at_month",
        "voltage_minus_kwh",
        "site_tariff",
        "rows_in_read_at_year_site_id",
        "kwh_percent_rank",
        "voltage_minus_kwh_max_2",
        "kwh_change",
        F.max("voltage_minus_kwh").over(w_quality).alias("quality_max_voltage_minus_kwh"),
        F.ntile(4).over(w_distinct_quality_in_read_at).alias("kwh_change_quartile"),
        (F.col("kwh_change") - F.lag("kwh_change").over(w_site_tariff_by_next2_read_at)).alias("kwh_change_change"),
    )
)

stats = (
    sequenced_2
    .filter(F.col("kwh_percent_rank") > 0.2)
    .groupBy("site_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("n_rows") >= 2)
)

with_windows = (
    stats
    .select(
        "site_id",
        "n_rows",
        "distinct_ids",
        F.rank().over(w_n_rows_by_n_rows_ties).alias("n_rows_rank"),
        F.dense_rank().over(w_n_rows_by_n_rows_ties).alias("n_rows_dense_rank"),
        F.round(F.col("n_rows") / F.nullif(F.sum("n_rows").over(w_n_rows), F.lit(0)), 4).alias("n_rows_share_of_n_rows"),
    )
)

result = (
    with_windows
    .select(
        "site_id",
        "n_rows",
        "distinct_ids",
        "n_rows_rank",
        "n_rows_dense_rank",
        F.round(F.col("n_rows_share_of_n_rows") * 1.1, 2).alias("n_rows_share_of_n_rows_adj"),
    )
    .orderBy("n_rows_share_of_n_rows_adj")
)
