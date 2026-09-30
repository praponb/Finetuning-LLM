from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter((~F.col("quality").isin("OK", "EST")) & (F.col("kwh") >= 24.5))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
    )
)

without_matches = (
    filtered_readings
    .crossJoin(filtered_readings.agg(F.max("kwh").alias("overall_max_kwh")))
    .filter(F.col("kwh") < F.col("overall_max_kwh"))
    .drop("overall_max_kwh")
)

result = (
    without_matches
    .groupingSets(
        [["site_id", "quality"], ["site_id"], []],
        "site_id", "quality",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("quality") != "EST", F.col("voltage"))).alias("voltage_quality_est"),
        F.sum(F.when(F.col("quality") == "OK", 1).otherwise(0)).alias("n_quality_ok"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
    )
    .orderBy(F.col("site_id").asc_nulls_last(), F.col("quality").asc_nulls_last())
)
