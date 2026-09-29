from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_zone = Window.partitionBy("tariff", "zone")
w_tariff_by_commissioned_on_ru = Window.partitionBy("tariff").orderBy(F.col("commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tariff_by_days_since_commiss = Window.partitionBy("tariff").orderBy("days_since_commissioned_on")
w_tariff_by_days_since_commiss_2 = Window.partitionBy("tariff").orderBy("days_since_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_zone_tariff_by_days_since_co = Window.partitionBy("zone", "tariff").orderBy(F.col("days_since_commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tariff_by_commissioned_on_ti = Window.partitionBy("tariff").orderBy(F.col("commissioned_on").desc())

filtered_sites = (
    df_sites
    .filter((F.col("commissioned_on") >= F.lit("2024-09-15").cast("date")) & (F.length(F.trim(F.col("site_name"))) >= 7))
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on"),
    )
)

with_windows = (
    filtered_sites
    .select(
        "id",
        "zone",
        "commissioned_on",
        "days_since_commissioned_on",
        F.count(F.lit(1)).over(w_tariff_zone).alias("rows_in_tariff_zone"),
        F.sum("days_since_commissioned_on").over(w_tariff_by_commissioned_on_ru).alias("running_days_since_commissioned_on"),
        F.dense_rank().over(w_tariff_by_days_since_commiss).alias("days_since_commissioned_on_dense_rank"),
        F.count(F.lit(1)).over(w_tariff_by_days_since_commiss_2).alias("running_count"),
        F.sum("days_since_commissioned_on").over(w_zone_tariff_by_days_since_co).alias("running_days_since_commissioned_on_2"),
        F.round(F.col("installed_kw") - F.avg("installed_kw").over(w_tariff_zone), 2).alias("installed_kw_vs_tariff_zone_avg"),
        F.round(F.cume_dist().over(w_tariff_by_commissioned_on_ti), 3).alias("commissioned_on_cume_dist"),
    )
)

leaders = with_windows.filter(F.col("days_since_commissioned_on_dense_rank") <= 3)

result = (
    leaders
    .filter(F.col("running_days_since_commissioned_on") <= 4550)
    .select(
        "id",
        "zone",
        "commissioned_on",
        "days_since_commissioned_on",
        "rows_in_tariff_zone",
        "running_days_since_commissioned_on",
        "days_since_commissioned_on_dense_rank",
        "running_count",
        "running_days_since_commissioned_on_2",
        "commissioned_on_cume_dist",
        F.length(F.trim(F.col("zone"))).alias("zone_len"),
        F.date_format(F.col("commissioned_on"), "MMM yyyy").alias("commissioned_on_label"),
    )
    .orderBy("zone", "rows_in_tariff_zone")
)
