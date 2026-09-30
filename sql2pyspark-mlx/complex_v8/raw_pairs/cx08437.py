from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality = Window.partitionBy("site_id", "quality")
w_site_id_by_avg_kwh_adj = Window.partitionBy("site_id").orderBy("avg_kwh_adj", "id")

cleaned_readings = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
        F.round(F.col("kwh") * 1.1, 2).alias("kwh_adj"),
    )
)

quality_stats = (
    cleaned_readings
    .groupBy("quality")
    .agg(F.avg("kwh_adj").alias("avg_kwh_adj"))
)

outliers = (
    cleaned_readings.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.kwh_adj") > F.col("g.avg_kwh_adj"))
    .select(
        "s.*",
        "g.avg_kwh_adj",
        F.round(F.col("s.kwh_adj") - F.col("g.avg_kwh_adj"), 2).alias("kwh_adj_minus_avg"),
    )
)

metrics = (
    outliers
    .select(
        "*",
        F.count(F.lit(1)).over(w_site_id_quality).alias("rows_in_site_id_quality"),
        F.lag("read_at", 2).over(w_site_id_by_avg_kwh_adj).alias("prev2_read_at"),
        F.lead("kwh", 2).over(w_site_id_by_avg_kwh_adj).alias("next2_kwh"),
    )
)

qualified = (
    metrics.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") < 79.7), "left_anti")
)

result = (
    qualified
    .select(
        "id",
        "kwh",
        "kwh_adj_minus_avg",
        "least_voltage_kwh",
        "avg_kwh_adj",
    )
    .unpivot(
        ids=["id"],
        values=["kwh", "kwh_adj_minus_avg", "least_voltage_kwh", "avg_kwh_adj"],
        variableColumnName="metric",
        valueColumnName="value",
    )
)
