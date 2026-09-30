from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_read_at_to_date = Window.partitionBy(F.to_date("read_at"))

filtered_readings = (
    df_meter_readings
    .filter(F.col("kwh") > 35.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
        F.month(F.col("read_at")).alias("read_at_month"),
    )
)

combined_details = (
    filtered_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff") == "TOU"), "inner")
    .filter(~F.col("si.zone").isin("Z3", "Z2"))
    .select("s.*", F.col("si.zone").alias("site_zone"))
)

site_zone_stats = (
    combined_details
    .groupBy("site_zone")
    .agg(F.avg("read_at_month").alias("avg_read_at_month"))
)

above_average = (
    combined_details.alias("s")
    .join(site_zone_stats.alias("g"), F.col("g.site_zone") == F.col("s.site_zone"), "inner")
    .filter(F.col("s.read_at_month") > F.col("g.avg_read_at_month"))
    .select(
        "s.*",
        "g.avg_read_at_month",
        F.round(F.col("s.read_at_month") - F.col("g.avg_read_at_month"), 2).alias("read_at_month_minus_avg"),
    )
)

metrics = (
    above_average
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "quality_clean",
        "read_at_month_minus_avg",
        F.round(F.cume_dist().over(w_all_by_kwh), 3).alias("kwh_cume_dist"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_to_date), F.lit(0)), 4).alias("kwh_share_of_read_at_to_date"),
        F.min("avg_read_at_month").over(w_read_at_to_date).alias("read_at_to_date_min_avg_read_at_month"),
    )
)

result = (
    metrics
    .groupingSets(
        [["site_id", "quality"], ["site_id"], []],
        "site_id", "quality",
    )
    .agg(
        F.min("voltage").alias("min_voltage"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
    )
    .select(
        "site_id",
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "min_voltage",
        "n_rows",
        "quality_list",
    )
    .orderBy(F.col("site_id").asc_nulls_last(), F.col("quality").asc_nulls_last())
)
