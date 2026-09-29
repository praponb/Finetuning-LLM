from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_ids_filled_by_disti = Window.partitionBy("distinct_ids_filled").orderBy(F.col("distinct_ids_filled").desc(), "hired_on_year_month", "tier")
w_distinct_ids_filled_by_disti_2 = Window.partitionBy("distinct_ids_filled").orderBy(F.col("distinct_ids_filled").desc(), "hired_on_year_month", "tier").rowsBetween(-2, Window.currentRow)
w_distinct_ids_filled_by_disti_3 = Window.partitionBy("distinct_ids_filled").orderBy(F.col("distinct_ids_filled").desc())
w_distinct_ids_filled_by_disti_4 = Window.partitionBy("distinct_ids_filled").orderBy(F.col("distinct_ids_filled").desc(), "hired_on_year_month", "tier").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_support_agents
    .filter((F.year(F.col("hired_on")) == 2024) & (F.col("agent_name").isNotNull()))
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
        F.date_trunc("year", F.col("hired_on")).alias("hired_on_year"),
    )
)

summary = (
    base
    .filter(F.col("hired_on") < F.lit("2025-02-26").cast("date"))
    .groupBy(
        F.date_trunc("month", "hired_on_year").alias("hired_on_year_month"),
        "tier",
    )
    .agg(
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("tier").isin("T3", "T2"), 1)).alias("n_tier_in_t3_t2"),
    )
)

qualified = (
    summary
    .crossJoin(summary.agg(F.min("n_tier_in_t3_t2").alias("overall_min_n_tier_in_t3_t2")))
    .filter(F.col("n_tier_in_t3_t2") > F.col("overall_min_n_tier_in_t3_t2"))
    .drop("overall_min_n_tier_in_t3_t2")
)

labeled = (
    qualified
    .select(
        "hired_on_year_month",
        "tier",
        (F.floor(F.col("n_tier_in_t3_t2") / 2) * 2).alias("n_tier_in_t3_t2_bucket"),
        F.coalesce(F.col("distinct_ids"), F.lit(0)).alias("distinct_ids_filled"),
    )
)

with_windows = (
    labeled
    .select(
        "*",
        F.lag("hired_on_year_month", 3).over(w_distinct_ids_filled_by_disti).alias("prev3_hired_on_year_month"),
        F.round(F.avg("distinct_ids_filled").over(w_distinct_ids_filled_by_disti_2), 2).alias("distinct_ids_filled_avg_3"),
        F.row_number().over(w_distinct_ids_filled_by_disti).alias("seq_no"),
        F.rank().over(w_distinct_ids_filled_by_disti_3).alias("distinct_ids_filled_rank"),
        F.nth_value("hired_on_year_month", 2).over(w_distinct_ids_filled_by_disti_4).alias("second_hired_on_year_month"),
        F.lead("n_tier_in_t3_t2_bucket").over(w_distinct_ids_filled_by_disti).alias("next_n_tier_in_t3_t2_bucket"),
    )
)

enriched = (
    with_windows
    .filter(F.col("seq_no") > 2)
    .select(
        "*",
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
        F.abs(F.col("seq_no") - 7).alias("seq_no_distance"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("hired_on_year_month")).alias("days_since_hired_on_year_month"),
    )
)

result = (
    enriched
    .filter(F.month(F.col("hired_on_year_month")).isin(1, 2))
    .select(
        "hired_on_year_month",
        "tier",
        "n_tier_in_t3_t2_bucket",
        "distinct_ids_filled",
        "prev3_hired_on_year_month",
        "distinct_ids_filled_avg_3",
        "seq_no",
        "second_hired_on_year_month",
        "next_n_tier_in_t3_t2_bucket",
        "tier_or_unknown",
        "seq_no_distance",
        "days_since_hired_on_year_month",
        F.concat(F.col("tier"), F.lit("#"), F.col("distinct_ids_filled").cast("string")).alias("tier_ref"),
    )
)
