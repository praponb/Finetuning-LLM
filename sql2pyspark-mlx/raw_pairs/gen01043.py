from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts_date").desc(), "id")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 08:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        F.to_date("reading_ts").alias("reading_ts_date"),
    )
)

base = (
    filtered
    .filter(F.col("device_code").isin(["DV-03", "DV-04"]))
    .withColumn("years_since_reading_ts", (F.datediff(F.current_date(), "reading_ts") / 365))
    .withColumn("reading_ts_year", F.year("reading_ts"))
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "device_code",
        "humidity",
        "firmware",
        "reading_ts_date",
        "years_since_reading_ts",
        "reading_ts_year",
    )
)

deduped_device_readings = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped_device_readings
    .groupBy(
        F.when(F.col("humidity") >= 82, "LARGE").otherwise("SMALL").alias("humidity_tier"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_rows"),
        F.round(F.sum("reading_ts_year") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("reading_ts_year_per_dv_01"),
        F.min("reading_ts_year").alias("min_reading_ts_year"),
        F.percentile_approx("reading_ts_year", [0.25, 0.5, 0.75]).alias("reading_ts_year_quartiles"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
    )
)
