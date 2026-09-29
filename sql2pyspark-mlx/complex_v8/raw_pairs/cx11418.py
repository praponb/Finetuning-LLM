from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at").desc(), F.col("id").desc())

valid_readings = (
    df_meter_readings
    .filter((F.col("quality").isin("BAD", "OK")) & (F.col("kwh") < 84.9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("voltage") - 227.1).alias("voltage_distance"),
    )
)

annotated = (
    valid_readings
    .filter(F.col("voltage_distance") > 85.8)
    .withColumn("quality_len", F.length(F.trim(F.col("quality"))))
)

latest = (
    annotated
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

annotated_2 = (
    latest
    .filter(F.col("voltage") > 215.7)
    .select(
        "*",
        F.abs(F.col("voltage_distance") - 85.8).alias("voltage_distance_distance"),
        F.length(F.trim(F.col("quality"))).alias("quality_len_2"),
    )
)

result = (
    annotated_2
    .groupingSets(
        [["quality", "quality_len", "quality_len_2"], ["quality"], ["quality_len"]],
        "quality", "quality_len", "quality_len_2",
    )
    .agg(
        F.min("voltage_distance").alias("min_voltage_distance"),
        F.count("*").alias("cnt"),
        F.countDistinct("quality").alias("distinct_qualitys"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "quality_len",
        "quality_len_2",
        "min_voltage_distance",
        "cnt",
        "distinct_qualitys",
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("quality_len").asc_nulls_last(),
        F.col("quality_len_2").asc_nulls_last(),
    )
)
