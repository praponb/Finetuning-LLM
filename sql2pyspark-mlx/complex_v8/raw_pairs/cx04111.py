from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.abs(F.col("voltage") - 223.1).alias("voltage_distance"),
    )
)

scored = (
    scoped_readings
    .filter(F.col("kwh") < 80.8)
    .select(
        "*",
        F.when(F.col("quality").isin("OK", "BAD"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
    )
)

annotated = (
    scored
    .select(
        "*",
        F.abs(F.col("voltage_distance") - 73.5).alias("voltage_distance_distance"),
        F.length(F.trim(F.col("quality_group"))).alias("quality_group_len"),
    )
)

result = (
    annotated
    .groupBy("site_id")
    .pivot("quality", ["BAD", "OK"])
    .agg(F.count("voltage_distance"))
    .orderBy("site_id")
)
