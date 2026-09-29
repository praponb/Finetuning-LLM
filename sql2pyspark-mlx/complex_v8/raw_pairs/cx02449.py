from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_voltage_tier = Window.partitionBy("quality", "voltage_tier")
w_voltage_tier_by_voltage = Window.partitionBy("voltage_tier").orderBy("voltage", "id")

cleaned = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(8, 10))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("voltage") >= 239.6, "A").when(F.col("voltage") >= 223.0, "B").otherwise("C").alias("voltage_tier"),
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
    )
)

windowed = (
    cleaned
    .withColumn("voltage_share_of_quality_voltage_tier", F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality_voltage_tier), F.lit(0)), 4))
    .withColumn("prev_greatest_kwh_voltage", F.lag("greatest_kwh_voltage").over(w_voltage_tier_by_voltage))
)

derived = (
    windowed
    .filter(F.col("read_at") >= F.lit("2025-01-19 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.when(F.col("voltage") >= 232.0, "large").when(F.col("voltage") >= 224.8, "medium").otherwise("small").alias("voltage_tier_2"),
    )
)

result = (
    derived
    .groupingSets(
        [["voltage_tier_2", "site_id"], ["voltage_tier_2"], []],
        "voltage_tier_2", "site_id",
    )
    .agg(F.grouping_id().alias("gid"), F.max("read_at").alias("last_read_at"))
    .orderBy(
        F.col("voltage_tier_2").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
