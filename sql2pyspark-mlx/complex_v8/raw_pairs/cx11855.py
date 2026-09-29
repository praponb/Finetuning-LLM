from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_date_month_event_ty = Window.partitionBy(F.date_trunc("month", "event_at_date"), "event_type").orderBy("event_at", "id")
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy("event_at_date")
w_event_at_date_month_event_ty_2 = Window.partitionBy(F.date_trunc("month", "event_at_date"), "event_type").orderBy("event_at")
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy("event_at_date", "id")
w_event_at_date_rank_tier = Window.partitionBy("event_at_date_rank_tier")
w_distinct_ticket_ids_by_cnt = Window.partitionBy("distinct_ticket_ids").orderBy(F.col("cnt").desc(), "prev3_event_at_day", "event_at_date_rank_tier")

prepared = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)

with_windows = (
    prepared
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_date",
        "actor_ref",
        F.row_number().over(w_event_at_date_month_event_ty).alias("rn"),
        F.round(F.cume_dist().over(w_ticket_id_event_type_by_even), 3).alias("event_at_date_cume_dist"),
        F.rank().over(w_event_at_date_month_event_ty_2).alias("event_at_rank"),
        F.lag("event_at").over(w_event_at_date_month_event_ty).alias("prev_event_at"),
        F.rank().over(w_ticket_id_event_type_by_even).alias("event_at_date_rank"),
        F.datediff(F.col("event_at_date"), F.lag("event_at_date").over(w_ticket_id_event_type_by_even_2)).alias("days_since_prev_event_at_date"),
        F.round(F.percent_rank().over(w_ticket_id_event_type_by_even_2), 3).alias("event_at_date_percent_rank"),
        F.round(F.percent_rank().over(w_ticket_id_event_type_by_even), 3).alias("event_at_date_percent_rank_2"),
        F.lag("event_at", 3).over(w_ticket_id_event_type_by_even_2).alias("prev3_event_at"),
    )
)

tagged = (
    with_windows
    .select(
        "*",
        F.greatest(F.col("rn"), F.col("event_at_date_rank")).alias("greatest_rn_event_at_date_rank"),
        F.when(F.col("event_at_date_rank") >= 9, "HIGH").when(F.col("event_at_date_rank") >= 4, "MEDIUM").otherwise("LOW").alias("event_at_date_rank_tier"),
        F.lower(F.trim(F.col("actor_ref"))).alias("actor_ref_clean"),
    )
)

per_group = (
    tagged
    .groupBy(
        F.to_date("prev3_event_at").alias("prev3_event_at_day"),
        "event_at_date_rank_tier",
    )
    .agg(
        F.round(F.sum("greatest_rn_event_at_date_rank") / F.nullif(F.sum("event_at_date_percent_rank"), F.lit(0)), 3).alias("greatest_rn_event_at_date_rank_to_event_at_date_"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
        F.count("*").alias("cnt"),
    )
)

with_trends = (
    per_group
    .withColumn("cnt_decile", F.ntile(10).over(w_distinct_ticket_ids_by_cnt))
    .withColumn("cnt_quintile", F.ntile(5).over(w_distinct_ticket_ids_by_cnt))
)

result = (
    with_trends
    .filter(F.col("distinct_ticket_ids") >= 2)
    .select(
        "prev3_event_at_day",
        "event_at_date_rank_tier",
        "greatest_rn_event_at_date_rank_to_event_at_date_",
        "distinct_ticket_ids",
        "cnt_quintile",
        (F.floor(F.col("cnt") / 2) * 2).alias("cnt_bucket"),
        F.when(F.col("distinct_ticket_ids") >= 7, "HIGH").when(F.col("distinct_ticket_ids") >= 5, "MEDIUM").otherwise("LOW").alias("distinct_ticket_ids_tier"),
    )
    .orderBy(
        F.col("event_at_date_rank_tier").desc_nulls_first(),
        "prev3_event_at_day",
    )
    .limit(5)
)
