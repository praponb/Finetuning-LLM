from pyspark.sql import functions as F

joined_ranges = (
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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

grouped = (
    joined_ranges
    .filter(F.col("kwh") < 104.8)
    .groupBy(F.to_date("read_at").alias("read_at_day"), "site_id", "zone")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.count(F.when(~F.col("zone").isin("Z3", "Z4"), 1)).alias("n_zone_not_in_z3_z4"),
        F.max("read_at").alias("last_read_at"),
    )
    .filter(F.col("cnt") >= 3)
)

with_details = (
    grouped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.read_at_day",
        "s.site_id",
        "s.zone",
        "s.distinct_site_ids",
        "s.n_zone_not_in_z3_z4",
        "s.last_read_at",
        F.col("si.tariff").alias("site_tariff"),
    )
)

scored = (
    with_details
    .select(
        "*",
        (F.floor(F.col("distinct_site_ids") / 2) * 2).alias("distinct_site_ids_bucket"),
        F.substring(F.trim(F.col("zone")), 1, 2).alias("zone_prefix"),
    )
)

result = (
    scored
    .select(
        "read_at_day",
        "site_id",
        "distinct_site_ids_bucket",
        "n_zone_not_in_z3_z4",
        "distinct_site_ids",
    )
    .unpivot(
        ids=["read_at_day", "site_id"],
        values=["distinct_site_ids_bucket", "n_zone_not_in_z3_z4", "distinct_site_ids"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
)
