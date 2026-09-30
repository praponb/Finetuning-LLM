from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.abs(F.col("kwh") - 83.8).alias("kwh_distance"),
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
        F.round(F.col("kwh") * 0.25, 2).alias("kwh_adj"),
    )
)

tagged = (
    cleaned
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_clean",
        F.lower(F.trim(F.col("quality_clean"))).alias("quality_clean_clean"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

summarized_rows = (
    tagged.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    summarized_rows
    .groupingSets(
        [["site_id", "reading_count"], ["site_id"], []],
        "site_id", "reading_count",
    )
    .agg(
        F.max("voltage").alias("max_voltage"),
        F.round(F.sum("kwh"), 2).alias("total_kwh"),
    )
)
