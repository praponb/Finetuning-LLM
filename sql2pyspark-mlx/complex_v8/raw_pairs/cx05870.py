from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_claim_date_month_year_t = Window.partitionBy(F.year("last_claim_date_month"), "team").orderBy(F.col("max_level_minus_median").desc())
w_last_claim_date_month_year_t_2 = Window.partitionBy(F.year("last_claim_date_month"), "team").orderBy(F.col("max_level_minus_median").desc(), "team", "last_claim_date_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_adjusters
    .filter((F.col("level") < 3) & (F.col("team") == "Travel"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.min("reported_at").alias("first_claim_reported_at"),
    )
)

profiled = (
    scoped.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select("s.*", "c.last_claim_date", "c.first_claim_reported_at")
)

claim_stats_2 = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
        F.max("claim_date").alias("last_claim_date_2"),
    )
)

with_stats = (
    profiled.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select("s.*", "c.min_claim_claimed_amount", "c.last_claim_date_2")
)

team_stats = (
    with_stats
    .groupBy("team")
    .agg(F.percentile_approx("level", 0.5).alias("median_level"))
)

vs_group = (
    with_stats.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.level") >= F.col("g.median_level"))
    .select(
        "s.*",
        "g.median_level",
        F.round(F.col("s.level") - F.col("g.median_level"), 2).alias("level_minus_median"),
    )
)

summary = (
    vs_group
    .filter(F.col("last_claim_date") >= F.lit("2024-08-01").cast("date"))
    .groupBy(
        "team",
        F.date_trunc("month", "last_claim_date").alias("last_claim_date_month"),
    )
    .agg(
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
        F.max("level_minus_median").alias("max_level_minus_median"),
        F.count("*").alias("row_count"),
    )
)

with_trends = (
    summary
    .select(
        "*",
        F.round(F.percent_rank().over(w_last_claim_date_month_year_t), 3).alias("max_level_minus_median_percent_rank"),
        F.last("last_claim_date_month").over(w_last_claim_date_month_year_t_2).alias("last_last_claim_date_month"),
    )
)

result = (
    with_trends
    .filter(~F.col("team").isin("Property", "Travel"))
    .select(
        "team",
        "adjuster_name_list",
        "max_level_minus_median",
        "row_count",
        "last_last_claim_date_month",
        F.least(F.col("max_level_minus_median"), F.col("row_count")).alias("least_max_level_minus_median_row_count"),
    )
    .orderBy(
        F.col("least_max_level_minus_median_row_count").desc(),
        F.col("max_level_minus_median").desc_nulls_first(),
    )
)
