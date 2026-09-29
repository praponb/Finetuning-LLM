from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(8, 10))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

annotated = (
    filtered_readings
    .withColumn("read_at_plus_30m", F.col("read_at") + F.expr("INTERVAL 30 MINUTES"))
)

result = (
    annotated
    .groupingSets(
        [["site_id", "quality"], ["site_id"], ["quality"]],
        "site_id", "quality",
    )
    .agg(
        F.grouping("site_id").alias("is_total_site_id"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
