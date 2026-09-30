from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter((F.col("quality") == "EST") & (F.col("site_id") >= 2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
    )
)

without_matches = (
    valid_readings
    .crossJoin(valid_readings.agg(F.min("kwh").alias("overall_min_kwh")))
    .filter(F.col("kwh") > F.col("overall_min_kwh"))
    .drop("overall_min_kwh")
)

result = (
    without_matches
    .cube("site_id", "quality")
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("n_quality_bad"),
    )
    .select(
        "site_id",
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "quality_list",
        "n_quality_bad",
    )
    .orderBy(F.col("site_id").asc_nulls_last(), F.col("quality").asc_nulls_last())
)
