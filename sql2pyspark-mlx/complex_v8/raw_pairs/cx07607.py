from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("tariff", "rate_tier").orderBy(F.col("rate").desc(), F.col("id").desc())
w_valid_from_month_tariff_or_u = Window.partitionBy(F.date_trunc("month", "valid_from"), "tariff_or_unknown_2")
w_valid_to_month_by_rate_minus = Window.partitionBy(F.date_trunc("month", "valid_to")).orderBy("rate_minus_avg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_valid_to_month_by_rate_minus_2 = Window.partitionBy(F.date_trunc("month", "valid_to")).orderBy("rate_minus_avg", "id")
w_valid_to_year = Window.partitionBy("valid_to_year")
w_rate_minus_avg_seq_valid_to = Window.partitionBy("rate_minus_avg_seq", "valid_to_year").orderBy(F.col("running_avg_rate").desc(), "id")

valid = (
    df_tariff_windows
    .filter((F.col("rate") < 5.5) & (F.col("valid_from") < F.lit("2025-02-16 11:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        F.when(F.col("rate") >= 5.9, "large").when(F.col("rate") >= 3.7, "medium").otherwise("small").alias("rate_tier"),
    )
)

latest_per_key = (
    valid
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored = (
    latest_per_key
    .filter(F.col("tariff_or_unknown").isNotNull())
    .select(
        "*",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown_2"),
        F.year(F.col("valid_to")).alias("valid_to_year"),
    )
)

rate_tier_stats = (
    scored
    .groupBy("rate_tier")
    .agg(F.avg("rate").alias("avg_rate"))
)

outliers = (
    scored.alias("s")
    .join(rate_tier_stats.alias("g"), F.col("g.rate_tier") == F.col("s.rate_tier"), "inner")
    .filter(F.col("s.rate") > F.col("g.avg_rate"))
    .select(
        "s.*",
        "g.avg_rate",
        F.round(F.col("s.rate") - F.col("g.avg_rate"), 2).alias("rate_minus_avg"),
    )
)

with_windows = (
    outliers
    .select(
        "*",
        F.count(F.lit(1)).over(w_valid_from_month_tariff_or_u).alias("rows_in_valid_from_month_tariff_or_unknown_2"),
        F.sum("avg_rate").over(w_valid_to_month_by_rate_minus).alias("running_avg_rate"),
        F.row_number().over(w_valid_to_month_by_rate_minus_2).alias("rate_minus_avg_seq"),
        F.round(F.col("avg_rate") - F.avg("avg_rate").over(w_valid_from_month_tariff_or_u), 2).alias("avg_rate_vs_valid_from_month_tariff_or_unknown_2"),
    )
)

sequenced = (
    with_windows
    .select(
        "*",
        F.size(F.collect_set("rate_tier").over(w_valid_to_year)).alias("distinct_rate_tier_in_valid_to_year"),
        F.size(F.collect_set("tariff_or_unknown").over(w_valid_to_year)).alias("distinct_tariff_or_unknown_in_valid_to_year"),
        (F.col("rate") - F.lag("rate").over(w_rate_minus_avg_seq_valid_to)).alias("rate_change"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        "tariff_or_unknown",
        "rate_tier",
        "tariff_or_unknown_2",
        "valid_to_year",
        "avg_rate",
        "rate_minus_avg",
        "rows_in_valid_from_month_tariff_or_unknown_2",
        "avg_rate_vs_valid_from_month_tariff_or_unknown_2",
        "distinct_tariff_or_unknown_in_valid_to_year",
        "rate_change",
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
        F.lower(F.trim(F.col("rate_tier"))).alias("rate_tier_clean"),
    )
    .orderBy(F.col("rate").desc_nulls_first(), F.col("rate_minus_avg").desc(), "id")
    .limit(20)
)
