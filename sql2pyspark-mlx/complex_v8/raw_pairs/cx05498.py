from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_voltage = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id")
w_read_at_to_date = Window.partitionBy(F.to_date("read_at"))
w_site_tariff = Window.partitionBy("site_tariff")
w_site_id_by_voltage_full = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage_filled_runnin = Window.orderBy("voltage_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy(F.col("voltage").desc())
w_all_by_voltage_filled = Window.orderBy("voltage_filled", "id")
w_site_tariff_site_id_by_read = Window.partitionBy("site_tariff", "site_id").orderBy("read_at")
w_site_tariff_site_id_by_read_2 = Window.partitionBy("site_tariff", "site_id").orderBy("read_at", "id")
w_site_tariff_site_id_by_read_3 = Window.partitionBy("site_tariff", "site_id").orderBy("read_at", "id").rowsBetween(-3, Window.currentRow)
w_read_at_rank_2_voltage_fille = Window.partitionBy("read_at_rank_2", "voltage_filled_seq").orderBy("read_at_quintile", "id").rowsBetween(-6, -1)
w_all_by_voltage_prior_3 = Window.orderBy("voltage", "id").rowsBetween(-3, -1)
w_read_at_rank_2_voltage_fille_2 = Window.partitionBy("read_at_rank_2", "voltage_filled_seq").orderBy("read_at_quintile", "id")
w_read_at_rank_second_read_at = Window.partitionBy("read_at_rank", F.month("second_read_at"))

recent = (
    df_meter_readings
    .filter(F.col("quality").isin("EST", "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

with_details_site = (
    recent.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (~F.col("si.zone").isin("Z2", "Z4")), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.voltage_filled",
        F.col("si.tariff").alias("site_tariff"),
    )
)

sequenced = (
    with_details_site
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "voltage_filled",
        F.rank().over(w_site_id_by_voltage).alias("voltage_rank"),
        F.count(F.lit(1)).over(w_read_at_to_date).alias("rows_in_read_at_to_date"),
        F.count(F.lit(1)).over(w_site_tariff).alias("rows_in_site_tariff"),
        F.nth_value("read_at", 2).over(w_site_id_by_voltage_full).alias("second_read_at"),
        F.min("kwh").over(w_all_by_voltage_filled_runnin).alias("min_kwh_so_far"),
        F.round(F.percent_rank().over(w_site_id_by_voltage_ties), 3).alias("voltage_percent_rank"),
        F.min("kwh").over(w_site_tariff).alias("site_tariff_min_kwh"),
        F.rank().over(w_all_by_voltage_filled).alias("voltage_filled_rank"),
        F.rank().over(w_site_tariff_site_id_by_read).alias("read_at_rank"),
        F.rank().over(w_site_tariff_site_id_by_read_2).alias("read_at_rank_2"),
        F.row_number().over(w_all_by_voltage_filled).alias("voltage_filled_seq"),
        F.max("voltage").over(w_site_tariff_site_id_by_read_3).alias("voltage_max_4"),
        F.ntile(5).over(w_site_tariff_site_id_by_read_2).alias("read_at_quintile"),
        F.row_number().over(w_site_tariff_site_id_by_read_2).alias("read_at_seq"),
        F.lag("voltage_filled").over(w_site_tariff_site_id_by_read_2).alias("prev_voltage_filled"),
    )
)

with_windows = (
    sequenced
    .select(
        "*",
        F.sum("site_tariff_min_kwh").over(w_read_at_rank_2_voltage_fille).alias("site_tariff_min_kwh_sum_6_prior"),
        F.sum("prev_voltage_filled").over(w_all_by_voltage_prior_3).alias("prev_voltage_filled_sum_3_prior"),
        (F.col("site_tariff_min_kwh") - F.lag("site_tariff_min_kwh").over(w_read_at_rank_2_voltage_fille_2)).alias("site_tariff_min_kwh_change"),
        F.ntile(4).over(w_read_at_rank_2_voltage_fille_2).alias("read_at_quintile_quartile"),
        F.round(F.col("rows_in_read_at_to_date") - F.avg("rows_in_read_at_to_date").over(w_read_at_rank_second_read_at), 2).alias("rows_in_read_at_to_date_vs_read_at_rank_second_r"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "voltage_rank",
        "rows_in_read_at_to_date",
        "second_read_at",
        "voltage_percent_rank",
        "read_at_rank",
        "read_at_rank_2",
        "read_at_quintile",
        "prev_voltage_filled",
        "site_tariff_min_kwh_sum_6_prior",
        "prev_voltage_filled_sum_3_prior",
        "read_at_quintile_quartile",
        "rows_in_read_at_to_date_vs_read_at_rank_second_r",
        (F.col("rows_in_read_at_to_date") + F.col("voltage_filled_seq")).alias("rows_in_read_at_to_date_plus_voltage_filled_seq"),
    )
    .orderBy("rows_in_read_at_to_date")
)
