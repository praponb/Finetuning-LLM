from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_site_zone = Window.partitionBy(F.date_trunc("month", "read_at"), "site_zone")
w_quality_site_zone_by_voltage = Window.partitionBy("quality", "site_zone").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_zone_by_voltage_2 = Window.partitionBy("quality", "site_zone").orderBy("voltage")
w_quality_by_kwh = Window.partitionBy("quality").orderBy(F.col("kwh").desc(), "id")
w_quality_site_zone_by_voltage_3 = Window.partitionBy("quality", "site_zone").orderBy("voltage", "id")
w_site_zone_by_voltage_ties = Window.partitionBy("site_zone").orderBy("voltage")

recent = (
    df_meter_readings
    .filter(F.col("voltage").between(216.8, 241.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_parent = (
    recent.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff") == "TOU"), "left")
    .select("s.*", F.col("si.zone").alias("site_zone"), "si.site_name")
)

metrics = (
    with_parent
    .select(
        "*",
        F.count(F.lit(1)).over(w_read_at_month_site_zone).alias("rows_in_read_at_month_site_zone"),
        F.last("read_at", ignorenulls=True).over(w_quality_site_zone_by_voltage).alias("last_known_read_at"),
        F.rank().over(w_quality_site_zone_by_voltage_2).alias("voltage_rank"),
        F.size(F.collect_set("quality").over(w_read_at_month_site_zone)).alias("distinct_quality_in_read_at_month_site_zone"),
        F.lead("kwh").over(w_quality_by_kwh).alias("next_kwh"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_quality_by_kwh))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.ntile(3).over(w_quality_site_zone_by_voltage_3).alias("voltage_tercile"),
        F.round(F.avg("kwh").over(w_read_at_month_site_zone), 2).alias("read_at_month_site_zone_avg_kwh"),
        F.rank().over(w_site_zone_by_voltage_ties).alias("voltage_rank_2"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_month_site_zone), 2).alias("kwh_vs_read_at_month_site_zone_avg"),
    )
)

leaders = (
    metrics
    .filter(F.col("voltage_rank_2") <= 3)
    .drop("voltage_rank_2")
)

result = (
    leaders
    .select(
        "id",
        "site_id",
        "read_at",
        "site_name",
        "rows_in_read_at_month_site_zone",
        "voltage_rank",
        "distinct_quality_in_read_at_month_site_zone",
        "minutes_since_prev_read_at",
        "read_at_month_site_zone_avg_kwh",
        "kwh_vs_read_at_month_site_zone_avg",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("last_known_read_at")).alias("days_since_last_known_read_at"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
    .orderBy("minutes_since_prev_read_at")
)
