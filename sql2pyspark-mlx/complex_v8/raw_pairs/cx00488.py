from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("tariff_or_unknown", "tariff_group").orderBy(F.col("days_since_valid_to").desc(), F.col("id").desc())
w_valid_to_month_by_valid_from = Window.partitionBy(F.date_trunc("month", "valid_to")).orderBy(F.col("valid_from").desc(), "id")
w_tariff_group = Window.partitionBy("tariff_group")

cleaned = (
    df_tariff_windows
    .filter(~F.col("tariff").isin("FLAT", "TOU"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
    )
)

annotated = (
    cleaned
    .filter(F.hour(F.col("valid_to")).between(9, 12))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("valid_to")).alias("days_since_valid_to"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        F.when(F.col("tariff").isin("EV", "FLAT"), "group_a").when(F.col("tariff") == "TOU", "group_b").otherwise("other").alias("tariff_group"),
    )
)

unique_rows = (
    annotated
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_trends = (
    unique_rows
    .select(
        "*",
        F.ntile(10).over(w_valid_to_month_by_valid_from).alias("valid_from_decile"),
        F.round(F.avg("rate").over(w_tariff_group), 2).alias("tariff_group_avg_rate"),
    )
)

per_group = (
    with_trends
    .filter(F.col("days_since_valid_to") >= 140)
    .groupBy("valid_from_decile")
    .agg(
        F.round(F.stddev("valid_from_decile"), 2).alias("stddev_valid_from_decile"),
        F.round(F.avg("valid_from_decile"), 2).alias("avg_valid_from_decile"),
    )
)

filtered = (
    per_group
    .crossJoin(per_group.agg(F.avg("valid_from_decile").alias("overall_avg_valid_from_decile")))
    .filter(F.col("valid_from_decile") > F.col("overall_avg_valid_from_decile"))
    .drop("overall_avg_valid_from_decile")
)

labeled = (
    filtered
    .filter(F.col("valid_from_decile") > 5)
    .select(
        "valid_from_decile",
        "stddev_valid_from_decile",
        "avg_valid_from_decile",
        F.round(F.col("avg_valid_from_decile") * 1.07, 2).alias("avg_valid_from_decile_adj"),
        F.coalesce(F.col("valid_from_decile"), F.lit(0)).alias("valid_from_decile_filled"),
        F.abs(F.col("valid_from_decile") - 4).alias("valid_from_decile_distance"),
    )
)

result = (
    labeled
    .select(
        "valid_from_decile",
        "stddev_valid_from_decile",
        "avg_valid_from_decile",
        "avg_valid_from_decile_adj",
        "valid_from_decile_filled",
        "valid_from_decile_distance",
    )
    .orderBy(F.col("valid_from_decile_filled").desc())
)
