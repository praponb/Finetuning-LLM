from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_kwh_ties = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").asc_nulls_last())
w_all_by_read_at_running = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_least_kwh_rank_kwh_mo = Window.orderBy("least_kwh_rank_kwh", "id").rowsBetween(-6, Window.currentRow)
w_all_by_least_kwh_rank_kwh_fu = Window.orderBy("least_kwh_rank_kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_readings = (
    df_meter_readings
    .filter(F.col("kwh") <= 111.9)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

metrics = (
    scoped_readings
    .withColumn("kwh_rank", F.rank().over(w_quality_site_id_by_kwh_ties))
    .withColumn("first_known_read_at", F.first("read_at", ignorenulls=True).over(w_all_by_read_at_running))
)

annotated = (
    metrics
    .select(
        "*",
        F.least(F.col("kwh_rank"), F.col("kwh")).alias("least_kwh_rank_kwh"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

joined_site = (
    annotated.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.kwh_rank",
        "s.least_kwh_rank_kwh",
        "s.voltage_filled",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_parent = (
    joined_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone") != "Z3")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.kwh_rank",
        "s.least_kwh_rank_kwh",
        "s.voltage_filled",
        "s.site_tariff",
        "s.site_commissioned_on",
        "s.site_installed_kw",
        F.col("si.zone").alias("site_zone"),
    )
)

sequenced = (
    with_parent
    .select(
        "*",
        F.round(F.avg("kwh_rank").over(w_all_by_least_kwh_rank_kwh_mo), 2).alias("kwh_rank_avg_7"),
        F.nth_value("site_installed_kw", 2).over(w_all_by_least_kwh_rank_kwh_fu).alias("second_site_installed_kw"),
    )
)

result = (
    sequenced
    .cube("site_id", "kwh_rank", "quality")
    .agg(
        F.round(F.stddev("second_site_installed_kw"), 2).alias("stddev_second_site_installed_kw"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("site_zone").isNotNull(), F.col("kwh_rank_avg_7")).otherwise(0)).alias("kwh_rank_avg_7_site_zone_is_not_null"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("kwh_rank").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
