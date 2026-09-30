from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff_site_id_by_volta = Window.partitionBy("site_tariff", "site_id").orderBy("voltage", "id").rowsBetween(-1, -1)
w_site_id_site_tariff_by_volta = Window.partitionBy("site_id", "site_tariff").orderBy("voltage", "id").rowsBetween(-3, Window.currentRow)
w_site_id_site_tariff_by_volta_2 = Window.partitionBy("site_id", "site_tariff").orderBy("voltage", "id")
w_read_at_year = Window.partitionBy(F.year("read_at"))
w_site_tariff_site_id_by_volta_2 = Window.partitionBy("site_tariff", "site_id").orderBy("voltage", "id")
w_site_tariff = Window.partitionBy("site_tariff")
w_site_tariff_read_at_month_by = Window.partitionBy("site_tariff", F.month("read_at")).orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_site_id_quality_by_voltage_a = Window.partitionBy("site_id", "quality").orderBy("voltage_avg_4", "id")
w_quality_group_site_tariff_by = Window.partitionBy("quality_group", "site_tariff").orderBy(F.col("voltage_change").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_group_site_tariff_by_2 = Window.partitionBy("quality_group", "site_tariff").orderBy(F.col("voltage_change").asc_nulls_last(), "id")
w_voltage_seq_quality_group_by = Window.partitionBy("voltage_seq", "quality_group").orderBy(F.col("read_at_year_sum_voltage").desc_nulls_first(), "id")

valid = (
    df_meter_readings
    .filter((F.col("kwh") <= 116.8) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

combined_details_site = (
    valid.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
    )
)

metrics = (
    combined_details_site
    .select(
        "*",
        F.sum("voltage").over(w_site_tariff_site_id_by_volta).alias("voltage_sum_1_prior"),
        F.round(F.avg("voltage").over(w_site_id_site_tariff_by_volta), 2).alias("voltage_avg_4"),
        F.row_number().over(w_site_id_site_tariff_by_volta_2).alias("voltage_seq"),
        F.min("voltage").over(w_read_at_year).alias("read_at_year_min_voltage"),
        F.sum("voltage").over(w_read_at_year).alias("read_at_year_sum_voltage"),
        (F.col("voltage") - F.lag("voltage").over(w_site_tariff_site_id_by_volta_2)).alias("voltage_change"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_tariff), 2).alias("voltage_vs_site_tariff_avg"),
        F.sum("voltage").over(w_site_tariff_read_at_month_by).alias("voltage_sum_3_prior"),
    )
)

derived = (
    metrics
    .filter(F.col("site_id") <= 4)
    .select(
        "*",
        F.when(F.col("quality").isin("BAD", "OK"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
        (F.col("read_at") + F.expr("INTERVAL 1 DAYS")).alias("read_at_plus_1d"),
    )
)

ranked = (
    derived
    .select(
        "*",
        F.ntile(10).over(w_site_id_quality_by_voltage_a).alias("voltage_avg_4_decile"),
        F.sum("read_at_year_min_voltage").over(w_quality_group_site_tariff_by).alias("running_read_at_year_min_voltage"),
        F.round(F.percent_rank().over(w_site_id_quality_by_voltage_a), 3).alias("voltage_avg_4_percent_rank"),
        (F.col("voltage_sum_1_prior") - F.lag("voltage_sum_1_prior").over(w_quality_group_site_tariff_by_2)).alias("voltage_sum_1_prior_change"),
        F.dense_rank().over(w_voltage_seq_quality_group_by).alias("read_at_year_sum_voltage_dense_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "site_tariff",
        "site_name",
        "voltage_sum_1_prior",
        "voltage_avg_4",
        "read_at_year_min_voltage",
        "read_at_year_sum_voltage",
        "voltage_change",
        "voltage_vs_site_tariff_avg",
        "quality_group",
        "read_at_label",
        "read_at_plus_1d",
        "voltage_avg_4_decile",
        "running_read_at_year_min_voltage",
        "voltage_avg_4_percent_rank",
        "voltage_sum_1_prior_change",
        "read_at_year_sum_voltage_dense_rank",
        (F.col("read_at") + F.expr("INTERVAL 1 DAYS")).alias("read_at_plus_1d_2"),
    )
    .orderBy(
        F.col("voltage_avg_4_percent_rank").asc(),
        F.col("read_at").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
