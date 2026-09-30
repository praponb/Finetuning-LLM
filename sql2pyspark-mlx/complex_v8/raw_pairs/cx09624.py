from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_group_quality = Window.partitionBy("quality_group", "quality")
w_all_by_least_kwh_filled_kwh = Window.orderBy(F.col("least_kwh_filled_kwh").desc(), "id")
w_all_by_least_kwh_filled_kwh_2 = Window.orderBy(F.col("least_kwh_filled_kwh").desc(), "id").rowsBetween(-1, -1)
w_all_by_least_kwh_filled_kwh_3 = Window.orderBy(F.col("least_kwh_filled_kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_meter_readings
    .filter(F.col("kwh") < 84.9)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)

enriched = (
    scoped
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.when(F.col("quality").isin("OK", "BAD"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
        F.least(F.col("kwh_filled"), F.col("kwh")).alias("least_kwh_filled_kwh"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.round(F.col("kwh_filled") / F.nullif(F.sum("kwh_filled").over(w_quality_group_quality), F.lit(0)), 4).alias("kwh_filled_share_of_quality_group_quality"),
        F.row_number().over(w_all_by_least_kwh_filled_kwh).alias("least_kwh_filled_kwh_seq"),
        F.round(F.avg("kwh_filled").over(w_all_by_least_kwh_filled_kwh_2), 2).alias("kwh_filled_avg_1_prior"),
        F.round(F.percent_rank().over(w_all_by_least_kwh_filled_kwh), 3).alias("least_kwh_filled_kwh_percent_rank"),
        F.count(F.lit(1)).over(w_all_by_least_kwh_filled_kwh_3).alias("running_count"),
    )
)

enriched_2 = (
    with_windows
    .filter(F.col("quality_group").isNotNull())
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "greatest_kwh_voltage",
        "kwh_filled",
        "quality_prefix",
        "quality_slug",
        "least_kwh_filled_kwh",
        "kwh_filled_share_of_quality_group_quality",
        "least_kwh_filled_kwh_seq",
        "kwh_filled_avg_1_prior",
        "least_kwh_filled_kwh_percent_rank",
        F.round(F.col("kwh") / F.nullif(F.col("kwh_filled_avg_1_prior"), F.lit(0)), 3).alias("kwh_per_kwh_filled_avg_1_prior"),
    )
)

result = (
    enriched_2
    .filter(F.col("least_kwh_filled_kwh_percent_rank") >= 0.2)
    .select(
        "id",
        "kwh",
        "greatest_kwh_voltage",
        "kwh_filled",
        "quality_slug",
        "least_kwh_filled_kwh",
        "least_kwh_filled_kwh_seq",
        "kwh_filled_avg_1_prior",
        "least_kwh_filled_kwh_percent_rank",
        "kwh_per_kwh_filled_avg_1_prior",
        F.substring(F.trim(F.col("quality_slug")), 1, 3).alias("quality_slug_prefix"),
    )
)
