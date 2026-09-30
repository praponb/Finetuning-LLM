from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

result = (
    scoped
    .groupBy("site_id")
    .pivot("quality", ["EST", "BAD"])
    .agg(F.count("kwh_bucket"))
    .orderBy("site_id")
)
