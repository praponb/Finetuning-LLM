from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.when(F.col("kwh") >= 87.1, "A").when(F.col("kwh") >= 50.4, "B").otherwise("C").alias("kwh_tier"),
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

annotated = (
    prepared_readings
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "quality_len",
        "kwh_tier",
        "read_at_year",
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
    )
)

kwh_tier_stats = (
    annotated
    .groupBy("kwh_tier")
    .agg(F.avg("kwh").alias("avg_kwh"))
)

vs_group = (
    annotated.alias("s")
    .join(kwh_tier_stats.alias("g"), F.col("g.kwh_tier") == F.col("s.kwh_tier"), "inner")
    .filter(F.col("s.kwh") > F.col("g.avg_kwh"))
    .select(
        "s.*",
        "g.avg_kwh",
        F.round(F.col("s.kwh") - F.col("g.avg_kwh"), 2).alias("kwh_minus_avg"),
    )
)

result = (
    vs_group
    .select("id", "quality_len", "read_at_year")
    .unpivot(
        ids=["id"],
        values=["quality_len", "read_at_year"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
)
