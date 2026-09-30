from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_read_at_year_ties = Window.orderBy(F.col("read_at_year").desc())
w_all_by_least_voltage_kwh_run = Window.orderBy("least_voltage_kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_year_site_id_by_read = Window.partitionBy("read_at_year", "site_id").orderBy(F.col("read_at").desc(), "id")
w_read_at_year_site_id_by_read_2 = Window.partitionBy("read_at_year", "site_id").orderBy(F.col("read_at").desc())
w_read_at_rank_by_read_at_ties = Window.partitionBy("read_at_rank").orderBy("read_at")
w_read_at_rank_by_read_at_prio = Window.partitionBy("read_at_rank").orderBy("read_at", "id").rowsBetween(-6, -1)
w_quality_by_kwh = Window.partitionBy("quality").orderBy("kwh", "id")

filtered = (
    df_meter_readings
    .filter(~F.col("quality").isin("EST", "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

ranked = (
    filtered
    .select(
        "*",
        F.min("voltage").over(w_quality_site_id).alias("quality_site_id_min_voltage"),
        F.round(F.cume_dist().over(w_all_by_read_at_year_ties), 3).alias("read_at_year_cume_dist"),
        F.count(F.lit(1)).over(w_all_by_least_voltage_kwh_run).alias("running_count"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_read_at_year_site_id_by_read))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.rank().over(w_read_at_year_site_id_by_read_2).alias("read_at_rank"),
    )
)

top_rows = ranked.filter(F.col("read_at_rank") == 1)

with_windows = (
    top_rows
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "read_at_year",
        "quality_site_id_min_voltage",
        "read_at_year_cume_dist",
        "running_count",
        "minutes_since_prev_read_at",
        "read_at_rank",
        F.dense_rank().over(w_read_at_rank_by_read_at_ties).alias("read_at_dense_rank"),
        F.round(F.avg("read_at_year").over(w_read_at_rank_by_read_at_prio), 2).alias("read_at_year_avg_6_prior"),
        F.round(F.percent_rank().over(w_quality_by_kwh), 3).alias("kwh_percent_rank"),
    )
)

result = (
    with_windows
    .select("quality_site_id_min_voltage")
    .distinct()
    .orderBy(F.col("quality_site_id_min_voltage").desc_nulls_first())
)
