from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_event_types_least_d = Window.partitionBy("distinct_event_types", "least_distinct_event_types_row_count").orderBy("pct_event_type_is_not_null", "event_at_month", "event_type")
w_distinct_event_types_least_d_2 = Window.partitionBy("distinct_event_types", "least_distinct_event_types_row_count").orderBy("pct_event_type_is_not_null")
w_distinct_event_types_least_d_3 = Window.partitionBy("distinct_event_types", "least_distinct_event_types_row_count").orderBy("pct_event_type_is_not_null", "event_at_month", "event_type").rowsBetween(-6, Window.currentRow)

prepared = (
    df_ticket_events
    .filter((~F.col("actor").isin("agent", "system")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

totals = (
    prepared
    .filter(F.col("actor_or_unknown") != "customer")
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"), "event_type")
    .agg(
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.round(100 * F.count(F.when(F.col("event_type").isNotNull(), 1)) / F.count("*"), 1).alias("pct_event_type_is_not_null"),
        F.round(100 * F.count(F.when(F.col("event_type") == "resolved", 1)) / F.count("*"), 1).alias("pct_event_type_resolved"),
        F.count("*").alias("row_count"),
        F.count(F.when(~F.col("event_type").isin("escalated", "reopened"), 1)).alias("n_event_type_not_in_escalated"),
    )
    .filter(F.col("row_count") >= 2)
)

labeled = (
    totals
    .filter(F.col("row_count").between(4, 8))
    .select(
        "*",
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.least(F.col("distinct_event_types"), F.col("row_count")).alias("least_distinct_event_types_row_count"),
        F.when(F.col("distinct_event_types") >= 7, "A").when(F.col("distinct_event_types") >= 5, "B").otherwise("C").alias("distinct_event_types_tier"),
    )
)

annotated = (
    labeled
    .select(
        "*",
        F.greatest(F.col("distinct_event_types"), F.col("pct_event_type_resolved")).alias("greatest_distinct_event_types_pct_event_type_res"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("row_count").cast("string")).alias("event_type_ref"),
        F.dayofweek(F.col("event_at_month")).alias("event_at_month_dayofweek"),
    )
)

ranked = (
    annotated
    .select(
        "*",
        F.row_number().over(w_distinct_event_types_least_d).alias("pct_event_type_is_not_null_seq"),
        F.rank().over(w_distinct_event_types_least_d).alias("pct_event_type_is_not_null_rank"),
        F.lag("distinct_event_types").over(w_distinct_event_types_least_d).alias("prev_distinct_event_types"),
        F.round(F.percent_rank().over(w_distinct_event_types_least_d_2), 3).alias("pct_event_type_is_not_null_percent_rank"),
        F.round(F.avg("row_count").over(w_distinct_event_types_least_d_3), 2).alias("row_count_avg_7"),
    )
)

distinct_event_types_tier_stats = (
    ranked
    .groupBy("distinct_event_types_tier")
    .agg(F.avg("row_count_avg_7").alias("avg_row_count_avg_7"))
)

compared = (
    ranked.alias("s")
    .join(distinct_event_types_tier_stats.alias("g"), F.col("g.distinct_event_types_tier") == F.col("s.distinct_event_types_tier"), "inner")
    .filter(F.col("s.row_count_avg_7") > F.col("g.avg_row_count_avg_7"))
    .select(
        "s.*",
        "g.avg_row_count_avg_7",
        F.round(F.col("s.row_count_avg_7") - F.col("g.avg_row_count_avg_7"), 2).alias("row_count_avg_7_minus_avg"),
    )
)

result = (
    compared
    .cube("row_count", "prev_distinct_event_types", "least_distinct_event_types_row_count")
    .agg(
        F.round(F.stddev("pct_event_type_is_not_null_percent_rank"), 2).alias("stddev_pct_event_type_is_not_null_percent_rank"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("row_count").asc_nulls_last(),
        F.col("prev_distinct_event_types").asc_nulls_last(),
        F.col("least_distinct_event_types_row_count").asc_nulls_last(),
    )
)
