from pyspark.sql import functions as F

scoped = (
    df_warehouses
    .filter((F.col("opened_on") < F.lit("2025-02-14").cast("date")) & (F.col("wh_code").isNotNull()))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.initcap(F.trim(F.col("wh_code"))).alias("wh_code_clean"),
    )
)

stats = (
    scoped
    .groupBy(F.date_trunc("month", "opened_on").alias("opened_on_month"), "city")
    .agg(
        F.round(F.stddev("capacity"), 2).alias("stddev_capacity"),
        F.count(F.when(F.upper(F.trim(F.col("wh_code"))).like("B%"), 1)).alias("n_upper_trim_wh_code_like_b"),
        F.round(F.avg("capacity"), 2).alias("avg_capacity"),
        F.sum(F.when(F.col("city").isNotNull(), F.col("capacity"))).alias("capacity_city_is_not_null"),
        F.countDistinct("city").alias("distinct_citys"),
        F.max("capacity").alias("max_capacity_check"),
    )
    .filter(F.col("max_capacity_check") > 2614)
    .drop("max_capacity_check")
)

annotated = (
    stats
    .select(
        "opened_on_month",
        "city",
        "stddev_capacity",
        "avg_capacity",
        "distinct_citys",
        F.coalesce(F.col("distinct_citys"), F.lit(0)).alias("distinct_citys_filled"),
        F.when(F.col("distinct_citys") >= 7, "HIGH").when(F.col("distinct_citys") >= 4, "MEDIUM").otherwise("LOW").alias("distinct_citys_tier"),
        (F.col("avg_capacity") + F.col("stddev_capacity")).alias("avg_capacity_plus_stddev_capacity"),
    )
)

with_activity = (
    annotated
    .crossJoin(annotated.agg(F.avg("avg_capacity_plus_stddev_capacity").alias("overall_avg_avg_capacity_plus_stddev_capacity")))
    .filter(F.col("avg_capacity_plus_stddev_capacity") > F.col("overall_avg_avg_capacity_plus_stddev_capacity"))
    .drop("overall_avg_avg_capacity_plus_stddev_capacity")
)

result = (
    with_activity
    .select(
        "opened_on_month",
        "city",
        "stddev_capacity",
        "distinct_citys",
        "distinct_citys_filled",
        "distinct_citys_tier",
        "avg_capacity_plus_stddev_capacity",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_on_month")).alias("days_since_opened_on_month"),
        F.greatest(F.col("distinct_citys"), F.col("stddev_capacity")).alias("greatest_distinct_citys_stddev_capacity"),
    )
    .orderBy(
        F.col("avg_capacity_plus_stddev_capacity").asc(),
        F.col("distinct_citys").desc(),
    )
)
