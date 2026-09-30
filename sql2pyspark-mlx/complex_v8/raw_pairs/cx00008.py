from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_valid_to_date_year = Window.partitionBy(F.year("first_valid_to_date"))
w_last_valid_to_date_month_by = Window.partitionBy(F.date_trunc("month", "last_valid_to_date")).orderBy("last_valid_to", "valid_from_day", "rate_tier", "valid_to_ym").rowsBetween(-2, Window.currentRow)
w_last_valid_to_date_month_by_2 = Window.partitionBy(F.date_trunc("month", "last_valid_to_date")).orderBy("last_valid_to", "valid_from_day", "rate_tier", "valid_to_ym")
w_last_valid_to_date_month_by_3 = Window.partitionBy(F.date_trunc("month", "last_valid_to_date")).orderBy("last_valid_to_percent_rank")
w_first_valid_to_date_month_la = Window.partitionBy(F.month("first_valid_to_date"), F.to_date("last_valid_to"))
w_last_valid_to_date_month_by_4 = Window.partitionBy(F.date_trunc("month", "last_valid_to_date")).orderBy("last_valid_to_percent_rank", "valid_from_day", "rate_tier", "valid_to_ym")

recent = (
    df_tariff_windows
    .filter(F.to_date(F.col("valid_to")) < F.lit("2025-03-12").cast("date"))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.initcap(F.trim(F.col("tariff"))).alias("tariff_clean"),
        F.abs(F.col("rate") - 5.5).alias("rate_distance"),
    )
)

rollup_base = (
    recent
    .groupBy(
        F.to_date("valid_from").alias("valid_from_day"),
        F.when(F.col("rate") >= 5.9, "HIGH").when(F.col("rate") >= 3.7, "MEDIUM").otherwise("LOW").alias("rate_tier"),
        F.date_format("valid_to", "yyyy-MM").alias("valid_to_ym"),
    )
    .agg(
        F.max("rate").alias("max_rate"),
        F.round(F.avg("rate_distance"), 2).alias("avg_rate_distance"),
        F.count("*").alias("row_count"),
        F.min("valid_to").alias("first_valid_to"),
        F.max("valid_to").alias("last_valid_to"),
    )
)

scored = (
    rollup_base
    .select(
        "*",
        F.when(F.col("avg_rate_distance") >= 4.8, "large").when(F.col("avg_rate_distance") >= 1.6, "medium").otherwise("small").alias("avg_rate_distance_tier"),
        F.to_date(F.col("first_valid_to")).alias("first_valid_to_date"),
    )
)

tagged = (
    scored
    .select(
        "valid_from_day",
        "rate_tier",
        "valid_to_ym",
        "max_rate",
        "avg_rate_distance",
        "first_valid_to",
        "last_valid_to",
        "first_valid_to_date",
        F.to_date(F.col("last_valid_to")).alias("last_valid_to_date"),
    )
)

ranked = (
    tagged
    .select(
        "valid_from_day",
        "rate_tier",
        "valid_to_ym",
        "max_rate",
        "avg_rate_distance",
        "first_valid_to",
        "last_valid_to",
        "first_valid_to_date",
        "last_valid_to_date",
        F.round(F.col("avg_rate_distance") - F.avg("avg_rate_distance").over(w_first_valid_to_date_year), 2).alias("avg_rate_distance_vs_first_valid_to_date_year_av"),
        F.min("avg_rate_distance").over(w_first_valid_to_date_year).alias("first_valid_to_date_year_min_avg_rate_distance"),
        F.round(F.avg("max_rate").over(w_last_valid_to_date_month_by), 2).alias("max_rate_avg_3"),
        F.round(F.percent_rank().over(w_last_valid_to_date_month_by_2), 3).alias("last_valid_to_percent_rank"),
    )
)

windowed = (
    ranked
    .select(
        "valid_from_day",
        "rate_tier",
        "valid_to_ym",
        "max_rate",
        "avg_rate_distance",
        "first_valid_to",
        "first_valid_to_date",
        "avg_rate_distance_vs_first_valid_to_date_year_av",
        "max_rate_avg_3",
        "last_valid_to_percent_rank",
        F.round(F.cume_dist().over(w_last_valid_to_date_month_by_3), 3).alias("last_valid_to_percent_rank_cume_dist"),
        F.round(F.avg("max_rate_avg_3").over(w_first_valid_to_date_month_la), 2).alias("first_valid_to_date_month_last_valid_to_to_date_"),
        (F.col("max_rate") - F.lag("max_rate").over(w_last_valid_to_date_month_by_4)).alias("max_rate_change"),
        F.size(F.collect_set("rate_tier").over(w_first_valid_to_date_month_la)).alias("distinct_rate_tier_in_first_valid_to_date_month_"),
    )
)

with_activity = (
    windowed
    .crossJoin(windowed.agg(F.avg("max_rate_change").alias("overall_avg_max_rate_change")))
    .filter(F.col("max_rate_change") > F.col("overall_avg_max_rate_change"))
    .drop("overall_avg_max_rate_change")
)

result = (
    with_activity
    .groupBy("valid_from_day")
    .pivot("rate_tier", ["LOW", "MEDIUM"])
    .agg(F.sum("max_rate_change"))
)
