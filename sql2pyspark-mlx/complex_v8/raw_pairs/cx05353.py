from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_dedup_2 = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_distinct_ids_bucket_distinct = Window.partitionBy("distinct_ids_bucket", "distinct_ids")
w_all_by_row_count = Window.orderBy("row_count", "ticket_id", "actor", "event_type")
w_actor_by_distinct_ids = Window.partitionBy("actor").orderBy(F.col("distinct_ids").desc(), "ticket_id", "actor", "event_type")
w_all_by_row_count_full = Window.orderBy("row_count", "ticket_id", "actor", "event_type").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_ids_distinct_ids_bu = Window.partitionBy("distinct_ids", "distinct_ids_bucket")
w_all_by_distinct_ids_bucket = Window.orderBy(F.col("distinct_ids_bucket").desc(), "ticket_id", "actor", "event_type")

valid_events = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 40)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

deduped = (
    valid_events
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

unique_rows = (
    deduped
    .withColumn("row_num", F.row_number().over(w_dedup_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

stats = (
    unique_rows
    .groupBy("ticket_id", "actor", "event_type")
    .agg(
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("event_at").alias("last_event_at"),
    )
    .filter(F.col("row_count") >= 2)
)

enriched = (
    stats
    .filter(F.col("event_type") == "assigned")
    .select(
        "*",
        (F.floor(F.col("distinct_ids") / 2) * 2).alias("distinct_ids_bucket"),
        (F.col("distinct_ticket_ids") + F.col("row_count")).alias("distinct_ticket_ids_plus_row_count"),
        (F.col("last_event_at") + F.expr("INTERVAL 7 DAYS")).alias("last_event_at_plus_7d"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.round(F.col("distinct_ticket_ids") / F.nullif(F.sum("distinct_ticket_ids").over(w_distinct_ids_bucket_distinct), F.lit(0)), 4).alias("distinct_ticket_ids_share_of_distinct_ids_bucket"),
        F.max("distinct_ticket_ids").over(w_distinct_ids_bucket_distinct).alias("distinct_ids_bucket_distinct_ids_max_distinct_ti"),
        F.rank().over(w_all_by_row_count).alias("row_count_rank"),
        F.sum("distinct_ids").over(w_distinct_ids_bucket_distinct).alias("distinct_ids_bucket_distinct_ids_sum_distinct_id"),
        F.round(F.percent_rank().over(w_actor_by_distinct_ids), 3).alias("distinct_ids_percent_rank"),
        F.first("distinct_ticket_ids").over(w_all_by_row_count_full).alias("first_distinct_ticket_ids"),
    )
)

sequenced = (
    ranked
    .select(
        "*",
        F.count(F.lit(1)).over(w_distinct_ids_distinct_ids_bu).alias("rows_in_distinct_ids_distinct_ids_bucket"),
        F.round(F.col("distinct_ids_bucket") - F.avg("distinct_ids_bucket").over(w_distinct_ids_distinct_ids_bu), 2).alias("distinct_ids_bucket_vs_distinct_ids_distinct_ids"),
        (F.col("first_distinct_ticket_ids") - F.lag("first_distinct_ticket_ids").over(w_all_by_distinct_ids_bucket)).alias("first_distinct_ticket_ids_change"),
    )
)

result = (
    sequenced
    .select(
        "ticket_id",
        "actor",
        "event_type",
        "row_count",
        "distinct_ids",
        "last_event_at",
        "distinct_ticket_ids_plus_row_count",
        "last_event_at_plus_7d",
        "distinct_ticket_ids_share_of_distinct_ids_bucket",
        "distinct_ids_bucket_distinct_ids_max_distinct_ti",
        "row_count_rank",
        "distinct_ids_percent_rank",
        "first_distinct_ticket_ids",
        "rows_in_distinct_ids_distinct_ids_bucket",
        "first_distinct_ticket_ids_change",
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
    )
)
