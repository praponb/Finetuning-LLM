from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_commissioned_on_full = Window.orderBy("commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_commissioned_on_year = Window.partitionBy("commissioned_on_year")

base_sites = (
    df_sites
    .filter(F.col("zone").isin("Z4", "Z3"))
    .select(
        "id",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.year(F.col("commissioned_on")).alias("commissioned_on_year"),
        F.concat(F.col("zone"), F.lit("#"), F.col("id").cast("string")).alias("zone_ref"),
        F.concat_ws(" / ", F.col("tariff"), F.col("zone")).alias("tariff_zone_label"),
    )
)

with_trends = (
    base_sites
    .select(
        "id",
        "tariff",
        "commissioned_on",
        "tariff_zone_label",
        F.nth_value("installed_kw", 2).over(w_all_by_commissioned_on_full).alias("second_installed_kw"),
        F.round(F.col("commissioned_on_year") - F.avg("commissioned_on_year").over(w_commissioned_on_year), 2).alias("commissioned_on_year_vs_commissioned_on_year_avg"),
        F.max("installed_kw").over(w_commissioned_on_year).alias("commissioned_on_year_max_installed_kw"),
    )
)

result = (
    with_trends
    .select("id", "second_installed_kw", "commissioned_on_year_max_installed_kw")
    .unpivot(
        ids=["id"],
        values=["second_installed_kw", "commissioned_on_year_max_installed_kw"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
