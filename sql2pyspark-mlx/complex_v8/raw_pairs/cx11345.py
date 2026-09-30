from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_by_commissioned_on = Window.partitionBy("zone").orderBy("commissioned_on", "id")
w_zone_by_commissioned_on_runn = Window.partitionBy("zone").orderBy("commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_zone_by_commissioned_on_full = Window.partitionBy("zone").orderBy("commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_sites
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

filtered = (
    cleaned.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.hour(F.col("m.read_at")).between(8, 14)), "left_semi")
    .filter(F.col("zone").isin("Z2", "Z3"))
)

enriched = (
    filtered
    .filter(F.col("commissioned_on") < F.lit("2024-11-21").cast("date"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
        F.lower(F.trim(F.col("tariff"))).alias("tariff_clean"),
    )
)

with_trends = (
    enriched
    .select(
        "id",
        "site_name",
        "tariff",
        "commissioned_on",
        "tariff_clean",
        F.lag("commissioned_on", 2).over(w_zone_by_commissioned_on).alias("prev2_commissioned_on"),
        F.datediff(F.col("commissioned_on"), F.lag("commissioned_on").over(w_zone_by_commissioned_on)).alias("days_since_prev_commissioned_on"),
        F.row_number().over(w_zone_by_commissioned_on).alias("commissioned_on_seq"),
        F.first("commissioned_on", ignorenulls=True).over(w_zone_by_commissioned_on_runn).alias("first_known_commissioned_on"),
        F.nth_value("commissioned_on", 2).over(w_zone_by_commissioned_on_full).alias("second_commissioned_on"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_name",
        "commissioned_on",
        "tariff_clean",
        "prev2_commissioned_on",
        "commissioned_on_seq",
        "first_known_commissioned_on",
        "second_commissioned_on",
    )
    .orderBy(
        F.col("prev2_commissioned_on").asc_nulls_last(),
        F.col("commissioned_on").desc(),
    )
)
