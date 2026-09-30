from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.col("voltage") < 233.8) & (F.col("site_id") <= 5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
        (F.col("kwh") + F.col("voltage")).alias("kwh_plus_voltage"),
    )
)

labeled = (
    scoped
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "kwh_minus_voltage",
        "kwh_plus_voltage",
        F.round(F.col("kwh") * 1.07, 2).alias("kwh_adj"),
    )
)

annotated = (
    labeled
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

result = (
    annotated
    .groupingSets(
        [["quality", "site_id"], ["quality"], []],
        "quality", "site_id",
    )
    .agg(
        F.round(F.sum("kwh"), 2).alias("total_kwh"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("kwh_adj").alias("min_kwh_adj"),
    )
    .orderBy(F.col("quality").asc_nulls_last(), F.col("site_id").asc_nulls_last())
)
