from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("voltage_tier_or_unknown").orderBy(F.col("stddev_voltage").desc(), F.col("voltage_tier").desc())

base = (
    df_meter_readings
    .filter(~F.col("quality").isin("EST", "BAD"))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

grouped = (
    base
    .groupBy(
        F.when(F.col("voltage") >= 232.4, "large").when(F.col("voltage") >= 218.4, "medium").otherwise("small").alias("voltage_tier"),
    )
    .agg(
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.round(F.stddev("voltage"), 2).alias("stddev_voltage"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
    )
)

derived = (
    grouped
    .select(
        "voltage_tier",
        "distinct_qualitys",
        "stddev_voltage",
        "quality_list",
        F.coalesce(F.col("voltage_tier"), F.lit("UNKNOWN")).alias("voltage_tier_or_unknown"),
        F.initcap(F.trim(F.col("voltage_tier"))).alias("voltage_tier_clean"),
    )
)

scored = (
    derived
    .filter(F.col("stddev_voltage") > 85.8)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("voltage_tier")), " ", "_")).alias("voltage_tier_slug"),
        (F.col("distinct_qualitys") + F.col("stddev_voltage")).alias("distinct_qualitys_plus_stddev_voltage"),
    )
)

latest_per_key = (
    scored
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    latest_per_key
    .select(
        "*",
        (F.col("stddev_voltage") - F.col("distinct_qualitys")).alias("stddev_voltage_minus_distinct_qualitys"),
    )
)

screened = (
    enriched
    .crossJoin(enriched.agg(F.avg("stddev_voltage").alias("overall_avg_stddev_voltage")))
    .filter(F.col("stddev_voltage") > F.col("overall_avg_stddev_voltage"))
    .drop("overall_avg_stddev_voltage")
)

result = (
    screened
    .select(
        "voltage_tier",
        "distinct_qualitys",
        "stddev_voltage",
        "quality_list",
        "distinct_qualitys_plus_stddev_voltage",
        "stddev_voltage_minus_distinct_qualitys",
        F.abs(F.col("distinct_qualitys") - 5).alias("distinct_qualitys_distance"),
    )
    .orderBy(F.col("stddev_voltage_minus_distinct_qualitys").desc())
)
