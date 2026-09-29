from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_first_reading_read_at_year_b = Window.partitionBy(F.year("first_reading_read_at")).orderBy("last_reading_read_at")
w_first_reading_read_at_year_b_2 = Window.partitionBy(F.year("first_reading_read_at")).orderBy("last_reading_read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_site_tariff_by_last = Window.partitionBy("site_id", "site_tariff").orderBy("last_reading_read_at_dense_rank")
w_site_id_site_tariff_by_last_2 = Window.partitionBy("site_id", "site_tariff").orderBy("last_reading_read_at_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_site_tariff_by_last_3 = Window.partitionBy("site_id", "site_tariff").orderBy("last_reading_read_at_dense_rank", "id")
w_site_id_site_tariff_by_last_4 = Window.partitionBy("site_id", "site_tariff").orderBy("last_reading_read_at_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_site_tariff_by_last_5 = Window.partitionBy("site_id", "site_tariff").orderBy("last_reading_read_at_dense_rank", "id").rowsBetween(-6, -1)
w_all_by_last_reading_read_at = Window.orderBy("last_reading_read_at_dense_rank_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_last_reading_read_at_2 = Window.orderBy("last_reading_read_at_dense_rank_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_reading_read_at_2_mont = Window.partitionBy(F.month("first_reading_read_at_2"))
w_all_by_last_reading_read_at_3 = Window.orderBy("last_reading_read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
        F.abs(F.col("kwh") - 50.4).alias("kwh_distance"),
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.max("read_at").alias("last_reading_read_at"),
        F.min("read_at").alias("first_reading_read_at_2"),
    )
)

summarized_rows = (
    prepared_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.first_reading_read_at",
        "m.last_reading_read_at",
        "m.first_reading_read_at_2",
    )
)

metrics = (
    summarized_rows
    .select(
        "id",
        "site_id",
        "kwh",
        "least_voltage_kwh",
        "kwh_distance",
        "first_reading_read_at",
        "last_reading_read_at",
        "first_reading_read_at_2",
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.dense_rank().over(w_first_reading_read_at_year_b).alias("last_reading_read_at_dense_rank"),
        F.last("read_at_year").over(w_first_reading_read_at_year_b_2).alias("last_read_at_year"),
    )
)

joined = (
    metrics.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

metrics_2 = (
    joined
    .select(
        "id",
        "kwh_distance",
        "last_reading_read_at",
        "first_reading_read_at_2",
        "rows_in_site_id",
        "last_read_at_year",
        "site_zone",
        "site_installed_kw",
        F.round(F.percent_rank().over(w_site_id_site_tariff_by_last), 3).alias("last_reading_read_at_dense_rank_percent_rank"),
        F.nth_value("kwh", 2).over(w_site_id_site_tariff_by_last_2).alias("second_kwh"),
        F.lead("first_reading_read_at_2").over(w_site_id_site_tariff_by_last_3).alias("next_first_reading_read_at_2"),
        F.ntile(3).over(w_site_id_site_tariff_by_last_3).alias("last_reading_read_at_dense_rank_tercile"),
        F.first("first_reading_read_at", ignorenulls=True).over(w_site_id_site_tariff_by_last_4).alias("first_known_first_reading_read_at"),
        F.sum("rows_in_site_id").over(w_site_id_site_tariff_by_last_5).alias("rows_in_site_id_sum_6_prior"),
    )
)

with_windows = (
    metrics_2
    .withColumn("second_site_installed_kw", F.nth_value("site_installed_kw", 2).over(w_all_by_last_reading_read_at))
    .withColumn("running_kwh_distance", F.sum("kwh_distance").over(w_all_by_last_reading_read_at_2))
)

metrics_3 = (
    with_windows
    .withColumn("second_site_installed_kw_vs_first_reading_read_a", F.round(F.col("second_site_installed_kw") - F.avg("second_site_installed_kw").over(w_first_reading_read_at_2_mont), 2))
    .withColumn("second_last_reading_read_at", F.nth_value("last_reading_read_at", 2).over(w_all_by_last_reading_read_at_3))
)

result = (
    metrics_3
    .select(
        "id",
        F.col("last_reading_read_at_dense_rank_tercile").alias("last_reading_read_at_dense_rank_tercile_v"),
        F.col("rows_in_site_id").alias("m_rows_in_site_id"),
    )
    .unpivot(
        ids=["id"],
        values=["last_reading_read_at_dense_rank_tercile_v", "m_rows_in_site_id"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
