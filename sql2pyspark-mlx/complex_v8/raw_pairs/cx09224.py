from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_site_tariff = Window.partitionBy("site_id", "site_tariff")
w_all_by_read_at_ties = Window.orderBy(F.col("read_at").desc_nulls_first())

base = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-02-18 12:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
        F.when(F.col("kwh") >= 109.0, "large").when(F.col("kwh") >= 34.1, "medium").otherwise("small").alias("kwh_tier"),
    )
)

with_parent_site = (
    base.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") > 141.6), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_clean",
        "s.kwh_tier",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

metrics = (
    with_parent_site
    .select(
        "*",
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id_site_tariff), 2).alias("voltage_vs_site_id_site_tariff_avg"),
        F.count(F.lit(1)).over(w_site_id_site_tariff).alias("rows_in_site_id_site_tariff"),
        F.round(F.percent_rank().over(w_all_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.dense_rank().over(w_all_by_read_at_ties).alias("read_at_dense_rank"),
        F.size(F.collect_set("kwh_tier").over(w_site_id_site_tariff)).alias("distinct_kwh_tier_in_site_id_site_tariff"),
    )
)

top_per_group = metrics.filter(F.col("read_at_dense_rank") <= 3)

result = (
    top_per_group
    .rollup("kwh_tier", "site_zone", "read_at_dense_rank")
    .agg(
        F.sum(F.when(~F.col("quality").isin("OK", "EST"), F.col("kwh"))).alias("kwh_quality_not_in_ok_est"),
        F.max("read_at").alias("last_read_at"),
        F.sum(F.when(~F.col("quality").isin("BAD", "EST"), F.col("read_at_dense_rank"))).alias("read_at_dense_rank_quality_not_in_bad_est"),
    )
    .select(
        F.coalesce(F.col("kwh_tier"), F.lit("ALL")).alias("kwh_tier"),
        F.coalesce(F.col("site_zone"), F.lit("ALL")).alias("site_zone"),
        "read_at_dense_rank",
        "kwh_quality_not_in_ok_est",
        "last_read_at",
        "read_at_dense_rank_quality_not_in_bad_est",
    )
)
