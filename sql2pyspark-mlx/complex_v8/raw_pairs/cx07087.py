from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_event_types_by_dist = Window.partitionBy("distinct_event_types").orderBy(F.col("distinct_actors").desc(), "actor", "event_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_event_types_by_dist_2 = Window.partitionBy("distinct_event_types").orderBy(F.col("distinct_actors").desc())
w_all_by_distinct_ids = Window.orderBy("distinct_ids", "actor", "event_at_month")
w_distinct_event_types_event_a = Window.partitionBy("distinct_event_types", F.date_trunc("month", "event_at_month"))
w_all_by_second_cnt = Window.orderBy(F.col("second_cnt").desc(), "actor", "event_at_month")

valid = (
    df_ticket_events
    .filter((F.col("actor") == "agent") & (F.col("event_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

grouped = (
    valid
    .groupBy("actor", F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("event_type").alias("distinct_event_types"),
    )
)

metrics = (
    grouped
    .withColumn("second_cnt", F.nth_value("cnt", 2).over(w_distinct_event_types_by_dist))
    .withColumn("distinct_actors_cume_dist", F.round(F.cume_dist().over(w_distinct_event_types_by_dist_2), 3))
)

sequenced = (
    metrics
    .select(
        "*",
        (F.col("distinct_ids") - F.lag("distinct_ids").over(w_all_by_distinct_ids)).alias("distinct_ids_change"),
        F.round(F.avg("cnt").over(w_distinct_event_types_event_a), 2).alias("distinct_event_types_event_at_month_month_avg_cn"),
        F.sum("distinct_ids").over(w_distinct_event_types_event_a).alias("distinct_event_types_event_at_month_month_sum_di"),
        F.round(F.percent_rank().over(w_all_by_second_cnt), 3).alias("second_cnt_percent_rank"),
        F.lead("distinct_actors").over(w_all_by_second_cnt).alias("next_distinct_actors"),
        F.lead("distinct_ids", 3).over(w_all_by_second_cnt).alias("next3_distinct_ids"),
    )
)

result = (
    sequenced
    .select(
        "actor",
        "cnt",
        "distinct_actors",
        "distinct_ids",
        "distinct_event_types",
        "second_cnt",
        "distinct_actors_cume_dist",
        "distinct_ids_change",
        "distinct_event_types_event_at_month_month_avg_cn",
        "distinct_event_types_event_at_month_month_sum_di",
        "second_cnt_percent_rank",
        "next_distinct_actors",
        "next3_distinct_ids",
    )
    .orderBy(F.col("distinct_ids").desc(), F.col("second_cnt_percent_rank").desc())
)
