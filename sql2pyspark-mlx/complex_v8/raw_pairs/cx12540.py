from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_commissioned_on_month = Window.partitionBy(F.month("commissioned_on"))
w_all_by_commissioned_on = Window.orderBy("commissioned_on", "id")

cleaned = (
    df_sites
    .filter((~F.col("zone").isin("Z2", "Z3")) & (F.col("installed_kw") <= 373.3))
    .select(
        "id",
        "site_name",
        "installed_kw",
        "commissioned_on",
        F.lower(F.trim(F.col("zone"))).alias("zone_clean"),
    )
)

ranked = (
    cleaned
    .select(
        "*",
        F.count(F.lit(1)).over(w_commissioned_on_month).alias("rows_in_commissioned_on_month"),
        F.lag("installed_kw", 3, 0).over(w_all_by_commissioned_on).alias("prev3_installed_kw"),
        F.min("installed_kw").over(w_commissioned_on_month).alias("commissioned_on_month_min_installed_kw"),
    )
)

exploded_detail_readings = (
    ranked.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.commissioned_on",
        "s.zone_clean",
        "s.prev3_installed_kw",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

result = (
    exploded_detail_readings
    .select(
        "id",
        "reading_id",
        "prev3_installed_kw",
        "reading_kwh",
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["prev3_installed_kw", "reading_kwh"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .orderBy("id", "reading_id", "metric")
)
