from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id")
w_grp = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.round((F.datediff("starts_on", "ends_on") / 30), 1).alias("months_ends_on_to_starts_on"),
        F.initcap(F.trim("promo_code")).alias("promo_code_title"),
    )
)

cleaned = (
    filtered
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "months_ends_on_to_starts_on",
        "promo_code_title",
        F.last_day("starts_on").alias("starts_on_month_end"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    deduped
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        "months_ends_on_to_starts_on",
        "starts_on_month_end",
        F.min("months_ends_on_to_starts_on").over(w_grp).alias("running_min_months_ends_on_to_starts_on"),
        F.sum("months_ends_on_to_starts_on").over(w_grp).alias("running_sum_months_ends_on_to_starts_on"),
        F.max("months_ends_on_to_starts_on").over(w_grp).alias("running_max_months_ends_on_to_starts_on"),
        F.count("months_ends_on_to_starts_on").over(w_grp).alias("running_count_months_ends_on_to_starts_on"),
    )
)

valid = (
    metrics
    .filter(F.col("running_sum_months_ends_on_to_starts_on").isNotNull())
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        "months_ends_on_to_starts_on",
        "starts_on_month_end",
        "running_sum_months_ends_on_to_starts_on",
        "running_max_months_ends_on_to_starts_on",
        "running_count_months_ends_on_to_starts_on",
        F.datediff("ends_on", "starts_on_month_end").alias("days_starts_on_month_end_to_ends_on"),
    )
)

result = (
    valid
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        "months_ends_on_to_starts_on",
        "running_sum_months_ends_on_to_starts_on",
        "running_max_months_ends_on_to_starts_on",
        "running_count_months_ends_on_to_starts_on",
        "days_starts_on_month_end_to_ends_on",
    )
)
