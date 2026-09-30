from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_zone").orderBy(F.col("avg_kwh").desc(), F.col("id").desc())

valid_readings = (
    df_meter_readings
    .filter((F.col("site_id") <= 3) & (F.month(F.col("read_at")).isin(1, 3)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 0.9, 2).alias("kwh_adj"),
    )
)

filtered = (
    valid_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") > 14.4) & (F.col("m.quality").isin("OK", "EST")), "left_anti")
)

quality_stats = (
    filtered
    .groupBy("quality")
    .agg(F.avg("kwh").alias("avg_kwh"))
)

compared = (
    filtered.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.kwh") > F.col("g.avg_kwh"))
    .select(
        "s.*",
        "g.avg_kwh",
        F.round(F.col("s.kwh") - F.col("g.avg_kwh"), 2).alias("kwh_minus_avg"),
    )
)

with_parent_site = (
    compared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.kwh_adj",
        "s.avg_kwh",
        "s.kwh_minus_avg",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

unique_rows = (
    with_parent_site
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .select(
        "id",
        F.col("kwh_adj").alias("kwh_adj_v"),
        F.col("avg_kwh").alias("m_avg_kwh"),
        F.col("kwh_minus_avg").alias("m_kwh_minus_avg"),
    )
    .unpivot(
        ids=["id"],
        values=["kwh_adj_v", "m_avg_kwh", "m_kwh_minus_avg"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric")
)
