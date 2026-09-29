from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_actor_by_distinct_event = Window.partitionBy("cnt", "actor").orderBy("distinct_event_types_per_cnt", "actor")
w_distinct_event_types_cnt_by = Window.partitionBy("distinct_event_types", "cnt").orderBy(F.col("cnt").desc_nulls_first(), "actor").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_ticket_events
    .filter((F.col("actor").isin("customer", "system")) & (F.col("ticket_id") >= 26))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
        F.when(F.col("event_type").isin("created", "assigned"), "group_a").when(F.col("event_type") == "escalated", "group_b").otherwise("other").alias("event_type_group"),
    )
)

tagged = (
    filtered
    .filter(F.col("event_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp"))
    .select("*", F.length(F.trim(F.col("actor"))).alias("actor_len"))
)

event_type_group_stats = (
    tagged
    .groupBy("event_type_group")
    .agg(F.percentile_approx("actor_len", 0.5).alias("median_actor_len"))
)

outliers = (
    tagged.alias("s")
    .join(event_type_group_stats.alias("g"), F.col("g.event_type_group") == F.col("s.event_type_group"), "inner")
    .filter(F.col("s.actor_len") >= F.col("g.median_actor_len"))
    .select(
        "s.*",
        "g.median_actor_len",
        F.round(F.col("s.actor_len") - F.col("g.median_actor_len"), 2).alias("actor_len_minus_median"),
    )
)

rollup_base = (
    outliers
    .groupBy("actor")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("event_type").alias("distinct_event_types"),
    )
    .filter(F.col("cnt") >= 2)
)

tagged_2 = (
    rollup_base
    .select(
        "*",
        F.abs(F.col("cnt") - 6).alias("cnt_distance"),
        F.concat(F.col("actor"), F.lit("#"), F.col("distinct_event_types").cast("string")).alias("actor_ref"),
        F.round(F.col("distinct_event_types") / F.nullif(F.col("cnt"), F.lit(0)), 3).alias("distinct_event_types_per_cnt"),
    )
)

with_trends = (
    tagged_2
    .withColumn("distinct_event_types_change", F.col("distinct_event_types") - F.lag("distinct_event_types").over(w_cnt_actor_by_distinct_event))
    .withColumn("last_distinct_event_types_per_cnt", F.last("distinct_event_types_per_cnt").over(w_distinct_event_types_cnt_by))
)

without_matches = (
    with_trends
    .crossJoin(with_trends.agg(F.min("distinct_event_types").alias("overall_min_distinct_event_types")))
    .filter(F.col("distinct_event_types") > F.col("overall_min_distinct_event_types"))
    .drop("overall_min_distinct_event_types")
)

result = (
    without_matches
    .select(
        "actor",
        "cnt",
        "distinct_event_types",
        "distinct_event_types_per_cnt",
        "last_distinct_event_types_per_cnt",
    )
    .orderBy(
        F.col("last_distinct_event_types_per_cnt").desc_nulls_first(),
        F.col("actor").desc(),
    )
    .limit(20)
)
