from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & F.col("humidity").isNull())
    .select(
        "*",
        (F.col("pressure") + F.col("humidity")).alias("pressure_plus_humidity"),
        F.when(F.col("humidity") >= 63, "High").when(F.col("humidity") >= 34, "Medium").otherwise("Low").alias("humidity_level"),
    )
)

result = (
    filtered
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .groupingSets(
        [["activity_day", "device_code"], ["activity_day"], []],
        "activity_day",
        "device_code",
    )
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("humidity")).otherwise(0)).alias("dv_02_humidity"),
        F.grouping("activity_day").alias("grouping_level"),
    )
    .orderBy(F.col("activity_day").desc_nulls_first())
)
