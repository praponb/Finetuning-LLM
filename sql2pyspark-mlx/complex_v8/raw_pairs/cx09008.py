from pyspark.sql import functions as F

matched = (
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

labeled = (
    matched
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "read_at",
        "quality",
        "zone",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
        F.greatest(F.col("rate"), F.col("kwh")).alias("greatest_rate_kwh"),
    )
)

with_activity = (
    labeled
    .crossJoin(labeled.agg(F.avg("greatest_rate_kwh").alias("overall_avg_greatest_rate_kwh")))
    .filter(F.col("greatest_rate_kwh") > F.col("overall_avg_greatest_rate_kwh"))
    .drop("overall_avg_greatest_rate_kwh")
)

result = (
    with_activity
    .filter(F.col("days_since_read_at") > 280)
    .select(
        "id",
        "quality",
        "zone",
        "read_at_hour",
        "greatest_rate_kwh",
        F.coalesce(F.col("greatest_rate_kwh"), F.lit(0.0)).alias("greatest_rate_kwh_filled"),
        F.date_format(F.col("read_at_hour"), "yyyy-MM-dd HH:00").alias("read_at_hour_label"),
    )
    .orderBy(F.col("greatest_rate_kwh_filled").desc(), F.col("read_at_hour").desc())
)
