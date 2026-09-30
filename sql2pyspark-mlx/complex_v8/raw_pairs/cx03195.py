from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_kwh_tier_site_id = Window.partitionBy("kwh_tier", "site_id")
w_all_by_site_installed_kw_tie = Window.orderBy(F.col("site_installed_kw").desc())
w_site_id_by_kwh_ties = Window.partitionBy("site_id").orderBy(F.col("kwh").asc_nulls_last())
w_all_by_site_installed_kw_run = Window.orderBy(F.col("site_installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_readings = (
    df_meter_readings
    .filter(F.col("voltage") > 215.1)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 79.4, "HIGH").when(F.col("kwh") >= 45.0, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
    )
)

enriched = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") >= 96.9)
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.kwh_tier",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "site_id",
        "kwh",
        "site_installed_kw",
        F.count(F.lit(1)).over(w_kwh_tier_site_id).alias("rows_in_kwh_tier_site_id"),
        F.rank().over(w_all_by_site_installed_kw_tie).alias("site_installed_kw_rank"),
        F.rank().over(w_site_id_by_kwh_ties).alias("kwh_rank"),
        F.last("kwh", ignorenulls=True).over(w_all_by_site_installed_kw_run).alias("last_known_kwh"),
    )
)

enriched_2 = (
    with_windows
    .filter(F.col("rows_in_kwh_tier_site_id") < 17)
    .select(
        "*",
        (F.col("last_known_kwh") + F.col("site_installed_kw")).alias("last_known_kwh_plus_site_installed_kw"),
        F.coalesce(F.col("kwh_rank"), F.lit(0)).alias("kwh_rank_filled"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        "site_installed_kw",
        "last_known_kwh_plus_site_installed_kw",
        "last_known_kwh",
        "kwh",
    )
    .unpivot(
        ids=["id"],
        values=["site_installed_kw", "last_known_kwh_plus_site_installed_kw", "last_known_kwh", "kwh"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
