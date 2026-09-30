from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy("read_at", "id")

matched = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "left",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

latest_per_key = (
    matched
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

summary = (
    latest_per_key
    .groupBy("site_id", "zone")
    .agg(
        F.sum("rate").alias("total_rate"),
        F.array_sort(F.collect_set("zone")).alias("zone_list"),
        F.max("read_at").alias("last_read_at"),
        F.avg("rate").alias("avg_rate_check"),
    )
    .filter(F.col("avg_rate_check") > 2.2)
    .drop("avg_rate_check")
)

enriched_site = (
    summary.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.site_id",
        "s.zone",
        "s.total_rate",
        "s.zone_list",
        "s.last_read_at",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    enriched_site
    .groupingSets(
        [["site_id", "zone", "site_zone"], ["site_id"], []],
        "site_id", "zone", "site_zone",
    )
    .agg(F.max("site_commissioned_on").alias("last_site_commissioned_on"))
    .select(
        "site_id",
        F.coalesce(F.col("zone"), F.lit("ALL")).alias("zone"),
        F.coalesce(F.col("site_zone"), F.lit("ALL")).alias("site_zone"),
        "last_site_commissioned_on",
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("zone").asc_nulls_last(),
        F.col("site_zone").asc_nulls_last(),
    )
)
