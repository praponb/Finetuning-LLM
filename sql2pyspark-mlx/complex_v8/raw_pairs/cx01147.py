from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_or_unknown_b = Window.partitionBy("site_id", "quality_or_unknown").orderBy("voltage")
w_quality_len_site_id = Window.partitionBy("quality_len", "site_id")
w_site_id_quality_or_unknown_b_2 = Window.partitionBy("site_id", "quality_or_unknown").orderBy("voltage", "id")
w_latest = Window.partitionBy("quality", "quality_or_unknown").orderBy(F.col("site_commissioned_on").desc(), F.col("id").desc())

scoped = (
    df_meter_readings
    .filter((F.col("site_id") >= 7) | (F.hour(F.col("read_at")).between(8, 13)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown_2"),
    )
)

combined_details = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

metrics = (
    combined_details
    .select(
        "*",
        F.round(F.percent_rank().over(w_site_id_quality_or_unknown_b), 3).alias("voltage_percent_rank"),
        F.round(F.avg("site_installed_kw").over(w_quality_len_site_id), 2).alias("quality_len_site_id_avg_site_installed_kw"),
        F.round(F.cume_dist().over(w_site_id_quality_or_unknown_b), 3).alias("voltage_cume_dist"),
        F.lead("site_installed_kw").over(w_site_id_quality_or_unknown_b_2).alias("next_site_installed_kw"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality_len_site_id), 2).alias("kwh_vs_quality_len_site_id_avg"),
    )
)

with_activity = (
    metrics.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.month(F.col("m.read_at")).isin(1, 3)), "left_anti")
)

unique_rows = (
    with_activity
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        "quality_or_unknown",
        "quality_or_unknown_2",
        "site_commissioned_on",
        "voltage_cume_dist",
        F.quarter(F.col("site_commissioned_on")).alias("site_commissioned_on_quarter"),
    )
    .orderBy("voltage_cume_dist", "quality_or_unknown_2")
)
