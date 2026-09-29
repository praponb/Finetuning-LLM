from pyspark.sql import functions as F

valid_readings = df_meter_readings.filter(F.col("site_id").isNotNull())

scored = (
    valid_readings
    .filter(F.col("voltage") <= 231.7)
    .select(
        "*",
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

result = (
    scored
    .groupingSets(
        [["quality", "site_id"], ["quality"]],
        "quality", "site_id",
    )
    .agg(F.array_sort(F.collect_set("quality")).alias("quality_list"))
    .orderBy(F.col("quality").asc_nulls_last(), F.col("site_id").asc_nulls_last())
)
