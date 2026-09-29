from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_site_id = Window.partitionBy(F.year("read_at"), "site_id")

in_window = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "inner",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

metrics = (
    in_window
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "rate",
        F.round(F.avg("energy_cost").over(w_read_at_year_site_id), 2).alias("read_at_year_site_id_avg_energy_cost"),
        F.size(F.collect_set("quality").over(w_read_at_year_site_id)).alias("distinct_quality_in_read_at_year_site_id"),
        F.size(F.collect_set("zone").over(w_read_at_year_site_id)).alias("distinct_zone_in_read_at_year_site_id"),
        F.round(F.col("energy_cost") - F.avg("energy_cost").over(w_read_at_year_site_id), 2).alias("energy_cost_vs_read_at_year_site_id_avg"),
    )
)

derived = (
    metrics
    .select(
        "id",
        "site_id",
        "rate",
        "read_at_year_site_id_avg_energy_cost",
        "distinct_quality_in_read_at_year_site_id",
        "distinct_zone_in_read_at_year_site_id",
        "energy_cost_vs_read_at_year_site_id_avg",
        F.greatest(F.col("rate"), F.col("kwh")).alias("greatest_rate_kwh"),
        F.abs(F.col("rate") - 5.7).alias("rate_distance"),
        F.when(F.col("rate") >= 5.9, "large").when(F.col("rate") >= 4.2, "medium").otherwise("small").alias("rate_tier"),
    )
)

per_group = (
    derived
    .filter(F.col("site_id") <= 8)
    .groupBy(
        F.when(F.col("rate") >= 5.7, "A").when(F.col("rate") >= 5.2, "B").otherwise("C").alias("rate_tier_2"),
        "rate_tier",
        "distinct_zone_in_read_at_year_site_id",
    )
    .agg(
        F.sum("greatest_rate_kwh").alias("total_greatest_rate_kwh"),
        F.percentile_approx("greatest_rate_kwh", 0.5).alias("median_greatest_rate_kwh"),
        F.count("*").alias("row_count"),
        F.percentile_approx("rate_distance", 0.5).alias("median_rate_distance"),
        F.round(F.stddev("rate"), 2).alias("stddev_rate"),
        F.round(F.stddev("distinct_quality_in_read_at_year_site_id"), 2).alias("stddev_distinct_quality_in_read_at_year_site_id"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    per_group
    .select(
        "rate_tier_2",
        "distinct_zone_in_read_at_year_site_id",
        "total_greatest_rate_kwh",
        "median_greatest_rate_kwh",
        "row_count",
        "median_rate_distance",
        "stddev_rate",
        "stddev_distinct_quality_in_read_at_year_site_id",
    )
    .orderBy(F.col("median_greatest_rate_kwh").desc(), F.col("row_count").desc())
)
