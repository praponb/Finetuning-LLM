from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter(F.col("kwh") >= 42.1)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.when(F.col("quality").isin("OK", "BAD"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
    )
)

qualified = (
    cleaned.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.site_name").isNotNull()), "left_anti")
)

enriched = (
    qualified
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "kwh",
        "quality",
        "quality_group",
        "voltage_plus_kwh",
        "least_kwh_voltage",
        F.length(F.trim(F.col("quality_group"))).alias("quality_group_len"),
        F.concat_ws("-", F.col("quality_group"), F.col("quality")).alias("quality_group_quality_label"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

filtered = (
    enriched
    .crossJoin(enriched.agg(F.avg("kwh").alias("overall_avg_kwh")))
    .filter(F.col("kwh") > F.col("overall_avg_kwh"))
    .drop("overall_avg_kwh")
)

result = (
    filtered
    .groupingSets(
        [["quality_group", "quality_len"], ["quality_group"], []],
        "quality_group", "quality_len",
    )
    .agg(
        F.grouping("quality_group").alias("is_total_quality_group"),
        F.sum("quality_group_len").alias("total_quality_group_len"),
        F.count("*").alias("row_count"),
        F.sum("least_kwh_voltage").alias("total_least_kwh_voltage"),
    )
    .select(
        F.coalesce(F.col("quality_group"), F.lit("ALL")).alias("quality_group"),
        "quality_len",
        "is_total_quality_group",
        "total_quality_group_len",
        "row_count",
        "total_least_kwh_voltage",
    )
    .orderBy(
        F.col("quality_group").asc_nulls_last(),
        F.col("quality_len").asc_nulls_last(),
    )
)
