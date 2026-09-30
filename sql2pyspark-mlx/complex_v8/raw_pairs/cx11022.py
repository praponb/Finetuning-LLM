from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_group_by_valid_to_ful = Window.partitionBy("tariff_group").orderBy("valid_to", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_group_by_valid_to_pri = Window.partitionBy("tariff_group").orderBy("valid_to", "id").rowsBetween(-2, -1)
w_tariff_group_rate_tier = Window.partitionBy("tariff_group", "rate_tier")
w_tariff_group_by_valid_to = Window.partitionBy("tariff_group").orderBy("valid_to", "id")
w_valid_from_year = Window.partitionBy("valid_from_year")
w_valid_from_year_valid_from_t = Window.partitionBy("valid_from_year", F.to_date("valid_from")).orderBy(F.col("valid_from").desc(), "id")
w_valid_from_year_valid_from_t_2 = Window.partitionBy("valid_from_year", F.to_date("valid_from")).orderBy(F.col("valid_from").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_tariff_windows
    .filter((~F.col("tariff").isin("FLAT", "EV")) | (F.to_date(F.col("valid_to")) < F.lit("2025-03-09").cast("date")))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.when(F.col("rate") >= 5.7, "HIGH").when(F.col("rate") >= 4.2, "MEDIUM").otherwise("LOW").alias("rate_tier"),
        F.when(F.col("tariff").isin("EV", "FLAT"), "group_a").when(F.col("tariff") == "TOU", "group_b").otherwise("other").alias("tariff_group"),
    )
)

windowed = (
    prepared
    .select(
        "id",
        "valid_from",
        "rate",
        "rate_tier",
        "tariff_group",
        F.first("rate").over(w_tariff_group_by_valid_to_ful).alias("first_rate"),
        F.round(F.avg("rate").over(w_tariff_group_by_valid_to_pri), 2).alias("rate_avg_2_prior"),
        F.round(F.col("rate") - F.avg("rate").over(w_tariff_group_rate_tier), 2).alias("rate_vs_tariff_group_rate_tier_avg"),
        F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_tariff_group_rate_tier), F.lit(0)), 4).alias("rate_share_of_tariff_group_rate_tier"),
        F.lead("rate", 2).over(w_tariff_group_by_valid_to).alias("next2_rate"),
    )
)

derived = (
    windowed
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("tariff_group")), " ", "_")).alias("tariff_group_slug"),
        F.date_trunc("month", F.col("valid_from")).alias("valid_from_month"),
        F.year(F.col("valid_from")).alias("valid_from_year"),
    )
)

windowed_2 = (
    derived
    .select(
        "*",
        F.sum("next2_rate").over(w_valid_from_year).alias("valid_from_year_sum_next2_rate"),
        F.round(F.cume_dist().over(w_valid_from_year_valid_from_t), 3).alias("valid_from_cume_dist"),
        F.last("rate").over(w_valid_from_year_valid_from_t_2).alias("last_rate"),
    )
)

result = (
    windowed_2
    .filter(F.col("rate_tier").isNotNull())
    .select("rate_share_of_tariff_group_rate_tier")
    .distinct()
    .orderBy(F.col("rate_share_of_tariff_group_rate_tier").desc())
)
