from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_or_unknown_tariff = Window.partitionBy("zone_or_unknown", "tariff")
w_zone_by_installed_kw_filled = Window.partitionBy("zone").orderBy("installed_kw_filled")
w_zone_by_installed_kw_filled_2 = Window.partitionBy("zone").orderBy("installed_kw_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_sites
    .filter(F.col("site_name").isNotNull())
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        F.coalesce(F.col("installed_kw"), F.lit(0.0)).alias("installed_kw_filled"),
        F.coalesce(F.col("installed_kw"), F.lit(0.0)).alias("installed_kw_filled_2"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on"),
    )
)

tagged = (
    valid
    .select(
        "*",
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
        F.substring(F.trim(F.col("zone")), 1, 4).alias("zone_prefix"),
    )
)

metrics = (
    tagged
    .select(
        "id",
        "tariff",
        "installed_kw",
        "days_since_commissioned_on",
        "zone_prefix",
        F.round(F.col("installed_kw_filled") - F.avg("installed_kw_filled").over(w_zone_or_unknown_tariff), 2).alias("installed_kw_filled_vs_zone_or_unknown_tariff_av"),
        F.count(F.lit(1)).over(w_zone_or_unknown_tariff).alias("rows_in_zone_or_unknown_tariff"),
        F.dense_rank().over(w_zone_by_installed_kw_filled).alias("installed_kw_filled_dense_rank"),
        F.rank().over(w_zone_by_installed_kw_filled).alias("installed_kw_filled_rank"),
        F.nth_value("days_since_commissioned_on", 2).over(w_zone_by_installed_kw_filled_2).alias("second_days_since_commissioned_on"),
        F.round(F.cume_dist().over(w_zone_by_installed_kw_filled), 3).alias("installed_kw_filled_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        F.col("installed_kw_filled_dense_rank").alias("m_installed_kw_filled_dense_rank"),
        F.col("rows_in_zone_or_unknown_tariff").alias("rows_in_zone_or_unknown_tariff_v"),
        F.col("second_days_since_commissioned_on").alias("second_days_since_commissioned_on_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_installed_kw_filled_dense_rank", "rows_in_zone_or_unknown_tariff_v", "second_days_since_commissioned_on_v"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
