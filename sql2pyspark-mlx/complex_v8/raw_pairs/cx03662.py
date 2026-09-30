from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("voltage") < 233.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

annotated = (
    scoped
    .filter(F.col("quality") == "OK")
    .select(
        "*",
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
    )
)

result = (
    annotated
    .groupingSets(
        [["site_id", "quality"], ["site_id"], ["quality"]],
        "site_id", "quality",
    )
    .agg(
        F.grouping("site_id").alias("is_total_site_id"),
        F.max("kwh_bucket").alias("max_kwh_bucket"),
        F.sum("kwh_bucket").alias("total_kwh_bucket"),
    )
    .select(
        "site_id",
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "is_total_site_id",
        "max_kwh_bucket",
        "total_kwh_bucket",
    )
    .orderBy(F.col("site_id").asc_nulls_last(), F.col("quality").asc_nulls_last())
)
