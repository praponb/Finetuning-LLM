from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_ids_by_first_closed = Window.partitionBy("distinct_ids").orderBy(F.col("first_closed_at").desc_nulls_first(), "channel").rowsBetween(-6, -1)
w_distinct_ids_channel = Window.partitionBy("distinct_ids", "channel")
w_distinct_ids_by_first_closed_2 = Window.partitionBy("distinct_ids").orderBy(F.col("first_closed_at").desc_nulls_first(), "channel")
w_next2_first_closed_at_to_dat = Window.partitionBy(F.to_date("next2_first_closed_at"))
w_distinct_ids_channel_by_dist = Window.partitionBy("distinct_ids", "channel").orderBy("distinct_ids_avg_6_prior")
w_distinct_ids_channel_by_dist_2 = Window.partitionBy("distinct_ids", "channel").orderBy("distinct_ids_avg_6_prior", "channel")
w_distinct_ids_by_distinct_ids = Window.partitionBy("distinct_ids").orderBy(F.col("distinct_ids_avg_6_prior").desc(), "channel")

scoped = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.array_contains(F.col("tags"), "refund")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

stats = (
    scoped
    .filter(F.col("channel_slug").isNotNull())
    .groupBy("channel")
    .agg(
        F.min("closed_at").alias("first_closed_at"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

filtered = (
    stats
    .crossJoin(stats.agg(F.avg("distinct_ids").alias("overall_avg_distinct_ids")))
    .filter(F.col("distinct_ids") > F.col("overall_avg_distinct_ids"))
    .drop("overall_avg_distinct_ids")
)

windowed = (
    filtered
    .select(
        "channel",
        "distinct_ids",
        F.round(F.avg("distinct_ids").over(w_distinct_ids_by_first_closed), 2).alias("distinct_ids_avg_6_prior"),
        F.lead("first_closed_at", 2).over(w_distinct_ids_by_first_closed_2).alias("next2_first_closed_at"),
        (F.col("distinct_ids") - F.lag("distinct_ids").over(w_distinct_ids_by_first_closed_2)).alias("distinct_ids_change"),
    )
)

windowed_2 = (
    windowed
    .select(
        "*",
        F.count(F.lit(1)).over(w_next2_first_closed_at_to_dat).alias("rows_in_next2_first_closed_at_to_date"),
        F.round(F.percent_rank().over(w_distinct_ids_channel_by_dist), 3).alias("distinct_ids_avg_6_prior_percent_rank"),
        F.dense_rank().over(w_distinct_ids_channel_by_dist_2).alias("distinct_ids_avg_6_prior_dense_rank"),
        F.lag("distinct_ids", 2).over(w_distinct_ids_by_distinct_ids).alias("prev2_distinct_ids"),
        F.dense_rank().over(w_distinct_ids_by_distinct_ids).alias("distinct_ids_avg_6_prior_dense_rank_2"),
    )
)

derived = windowed_2.withColumn("channel_clean", F.upper(F.trim(F.col("channel"))))

result = (
    derived
    .cube("channel", "distinct_ids_avg_6_prior_dense_rank")
    .agg(
        F.percentile_approx("distinct_ids_avg_6_prior_dense_rank", 0.5).alias("median_distinct_ids_avg_6_prior_dense_rank"),
        F.round(F.avg("distinct_ids_avg_6_prior"), 2).alias("avg_distinct_ids_avg_6_prior"),
    )
)
