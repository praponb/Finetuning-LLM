from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_read_at_prior_6 = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(-6, -1)
w_quality_by_rate_running = Window.partitionBy("quality").orderBy("rate", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

covered = (
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

quality_stats = (
    covered
    .groupBy("quality")
    .agg(F.max("kwh").alias("max_kwh"))
)

compared = (
    covered.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.kwh") == F.col("g.max_kwh"))
    .select(
        "s.*",
        "g.max_kwh",
        F.round(F.col("s.kwh") - F.col("g.max_kwh"), 2).alias("kwh_minus_max"),
    )
)

windowed = (
    compared
    .withColumn("energy_cost_avg_6_prior", F.round(F.avg("energy_cost").over(w_site_id_by_read_at_prior_6), 2))
    .withColumn("max_kwh_so_far", F.max("kwh").over(w_quality_by_rate_running))
)

result = (
    windowed
    .rollup("quality", "site_id", "zone")
    .agg(
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("kwh_minus_max"), 2).alias("stddev_kwh_minus_max"),
    )
)
