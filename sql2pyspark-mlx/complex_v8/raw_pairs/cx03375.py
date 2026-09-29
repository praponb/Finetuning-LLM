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

annotated = (
    matched
    .select(
        "*",
        F.when(F.col("zone").isin("Z2", "Z4"), "group_a").when(F.col("zone") == "Z3", "group_b").otherwise("other").alias("zone_group"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
        (F.floor(F.col("energy_cost") / 250) * 250).alias("energy_cost_bucket"),
    )
)

result = (
    annotated
    .cube("zone_group", "site_id")
    .agg(
        F.sum(F.when(F.col("quality").isNotNull(), F.col("energy_cost"))).alias("energy_cost_quality_is_not_null"),
        F.sum("energy_cost_bucket").alias("total_energy_cost_bucket"),
    )
    .orderBy(F.col("zone_group").asc_nulls_last(), F.col("site_id").asc_nulls_last())
)
