from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-03-08 09:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 96.8, "A").when(F.col("kwh") >= 45.3, "B").otherwise("C").alias("kwh_tier"),
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
        F.abs(F.col("voltage") - 225.2).alias("voltage_distance"),
    )
)

tagged = (
    valid
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

tagged_2 = (
    tagged
    .select(
        "*",
        (F.col("read_at") + F.expr("INTERVAL 2 HOURS")).alias("read_at_plus_2h"),
        F.round(F.col("kwh") * 1.1, 2).alias("kwh_adj"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
    )
)

rollup_base = (
    tagged_2
    .groupBy("site_id")
    .agg(
        F.sum(F.when(F.col("quality") == "BAD", 1).otherwise(0)).alias("n_quality_bad"),
        F.count("*").alias("cnt"),
        F.sum("kwh_adj").alias("total_kwh_adj"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.count(F.when(F.col("quality") != "BAD", 1)).alias("n_quality_bad_2"),
    )
)

result = (
    rollup_base
    .select(
        "site_id",
        F.col("cnt").alias("cnt_v"),
        F.col("n_quality_bad_2").alias("m_n_quality_bad_2"),
    )
    .unpivot(
        ids=["site_id"],
        values=["cnt_v", "m_n_quality_bad_2"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
