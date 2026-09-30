from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("event_type", "actor").orderBy(F.col("days_since_event_at").desc(), F.col("id").desc())
w_event_at_year_year_actor = Window.partitionBy(F.year("event_at_year"), "actor")
w_event_at_year_month_by_event = Window.partitionBy(F.month("event_at_year")).orderBy("event_at", "id").rowsBetween(-1, Window.currentRow)
w_event_at_year_month_by_event_2 = Window.partitionBy(F.month("event_at_year")).orderBy("event_at", "id").rowsBetween(-6, Window.currentRow)
w_dedup = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at_year").desc(), F.col("id").desc())

base = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 13)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

annotated = (
    base
    .filter(F.upper(F.trim(F.col("actor_clean"))).like("S%"))
    .select(
        "*",
        (F.floor(F.col("days_since_event_at") / 100) * 100).alias("days_since_event_at_bucket"),
        F.date_trunc("year", F.col("event_at")).alias("event_at_year"),
    )
)

latest = (
    annotated
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

windowed = (
    latest
    .select(
        "*",
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_event_at_year_year_actor), F.lit(0)), 4).alias("days_since_event_at_share_of_event_at_year_year_"),
        F.sum("days_since_event_at").over(w_event_at_year_month_by_event).alias("days_since_event_at_sum_2"),
        F.round(F.avg("days_since_event_at_bucket").over(w_event_at_year_year_actor), 2).alias("event_at_year_year_actor_avg_days_since_event_at"),
        F.round(F.avg("days_since_event_at_bucket").over(w_event_at_year_month_by_event_2), 2).alias("days_since_event_at_bucket_avg_7"),
        F.count(F.lit(1)).over(w_event_at_year_year_actor).alias("rows_in_event_at_year_year_actor"),
    )
)

tagged = (
    windowed
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_clean",
        "event_at_year",
        "days_since_event_at_share_of_event_at_year_year_",
        "days_since_event_at_sum_2",
        "event_at_year_year_actor_avg_days_since_event_at",
        "days_since_event_at_bucket_avg_7",
        F.round((F.unix_timestamp(F.col("event_at")) - F.unix_timestamp(F.col("event_at_year"))) / 60, 1).alias("minutes_event_at_year_to_event_at"),
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
)

deduped = (
    tagged
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

grouped = (
    deduped
    .filter(F.col("days_since_event_at_bucket_avg_7") < 525.0)
    .groupBy("event_type", "ticket_id", "actor")
    .agg(
        F.sum(F.when(F.col("actor").isin("system", "agent"), F.col("minutes_event_at_year_to_event_at"))).alias("minutes_event_at_year_to_event_at_actor_in_syste"),
        F.round(F.sum("days_since_event_at_share_of_event_at_year_year_") / F.nullif(F.sum("days_since_event_at_bucket_avg_7"), F.lit(0)), 3).alias("days_since_event_at_share_of_event_at_year_year__2"),
        F.sum("minutes_event_at_year_to_event_at").alias("total_minutes_event_at_year_to_event_at"),
        F.round(F.stddev("days_since_event_at_bucket_avg_7"), 2).alias("stddev_days_since_event_at_bucket_avg_7"),
    )
)

result = (
    grouped
    .select(
        "event_type",
        "ticket_id",
        "actor",
        "minutes_event_at_year_to_event_at_actor_in_syste",
        "total_minutes_event_at_year_to_event_at",
    )
    .orderBy(
        F.col("minutes_event_at_year_to_event_at_actor_in_syste").desc_nulls_first(),
        F.col("ticket_id").desc(),
        "event_type",
        "actor",
    )
    .limit(20)
)
