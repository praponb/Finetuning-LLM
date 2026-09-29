from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_plus_1d_to_date_by = Window.partitionBy(F.to_date("event_at_plus_1d")).orderBy(F.col("event_at").desc())
w_event_at_plus_1d_to_date_by_2 = Window.partitionBy(F.to_date("event_at_plus_1d")).orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at_plus = Window.partitionBy("event_type").orderBy(F.col("event_at_plus_1d").desc(), "id")
w_event_at_plus_1d_to_date_by_3 = Window.partitionBy(F.to_date("event_at_plus_1d")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_at_plus_1d_year_ticket = Window.partitionBy(F.year("event_at_plus_1d"), "ticket_id").orderBy("event_at", "id")
w_all_by_event_at_plus_1d = Window.orderBy("event_at_plus_1d", "id")
w_all_by_event_at_plus_1d_terc = Window.orderBy(F.col("event_at_plus_1d_tercile").desc(), "id").rowsBetween(-2, Window.currentRow)
w_event_at_seq_first_known_eve = Window.partitionBy("event_at_seq", F.to_date("first_known_event_at_plus_1d")).orderBy("event_at_plus_1d_tercile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_plus_1d_seq = Window.partitionBy("event_at_plus_1d_seq")
w_all_by_event_at_plus_1d_terc_2 = Window.orderBy(F.col("event_at_plus_1d_tercile").desc(), "id")
w_max_event_at_plus_1d_decile = Window.partitionBy("max_event_at_plus_1d_decile", "event_at_plus_1d_seq").orderBy("distinct_event_types", "event_at_plus_1d_seq")
w_max_event_at_plus_1d_decile_2 = Window.partitionBy("max_event_at_plus_1d_decile", "event_at_plus_1d_seq").orderBy("distinct_event_types", "event_at_plus_1d_seq").rowsBetween(-6, Window.currentRow)
w_max_event_at_plus_1d_decile_3 = Window.partitionBy("max_event_at_plus_1d_decile", "event_at_plus_1d_seq").orderBy("distinct_event_types", "event_at_plus_1d_seq").rowsBetween(-3, -1)
w_max_event_at_plus_1d_decile_4 = Window.partitionBy("max_event_at_plus_1d_decile", "event_at_plus_1d_seq").orderBy("distinct_event_types", "event_at_plus_1d_seq").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 9)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        (F.col("event_at") + F.expr("INTERVAL 1 DAYS")).alias("event_at_plus_1d"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_at_label",
        "event_at_plus_1d",
        F.round(F.cume_dist().over(w_event_at_plus_1d_to_date_by), 3).alias("event_at_cume_dist"),
        F.lead("event_at_plus_1d").over(w_event_at_plus_1d_to_date_by_2).alias("next_event_at_plus_1d"),
        F.round((F.unix_timestamp("event_at_plus_1d") - F.unix_timestamp(F.lag("event_at_plus_1d").over(w_event_type_by_event_at_plus))) / 60, 1).alias("minutes_since_prev_event_at_plus_1d"),
        F.ntile(3).over(w_event_type_by_event_at_plus).alias("event_at_plus_1d_tercile"),
        F.first("event_at_plus_1d", ignorenulls=True).over(w_event_at_plus_1d_to_date_by_3).alias("first_known_event_at_plus_1d"),
        F.ntile(10).over(w_event_type_by_event_at_plus).alias("event_at_plus_1d_decile"),
        F.first("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("first_known_event_at"),
        F.lag("event_at").over(w_event_type_by_event_at).alias("prev_event_at"),
        F.row_number().over(w_event_at_plus_1d_year_ticket).alias("event_at_seq"),
        F.dense_rank().over(w_event_at_plus_1d_to_date_by_2).alias("event_at_dense_rank"),
        F.row_number().over(w_all_by_event_at_plus_1d).alias("event_at_plus_1d_seq"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.round(F.avg("event_at_dense_rank").over(w_all_by_event_at_plus_1d_terc), 2).alias("event_at_dense_rank_avg_3"),
        F.last("prev_event_at", ignorenulls=True).over(w_event_at_seq_first_known_eve).alias("last_known_prev_event_at"),
        F.size(F.collect_set("event_type").over(w_event_at_plus_1d_seq)).alias("distinct_event_type_in_event_at_plus_1d_seq"),
        F.ntile(5).over(w_all_by_event_at_plus_1d_terc_2).alias("event_at_plus_1d_tercile_quintile"),
    )
)

per_group = (
    sequenced_2
    .groupBy("event_at_plus_1d_seq")
    .agg(
        F.max("first_known_event_at_plus_1d").alias("last_first_known_event_at_plus_1d"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.max("event_at_plus_1d_decile").alias("max_event_at_plus_1d_decile"),
        F.round(F.sum("minutes_since_prev_event_at_plus_1d"), 2).alias("total_minutes_since_prev_event_at_plus_1d"),
    )
)

with_trends = (
    per_group
    .select(
        "*",
        F.ntile(3).over(w_max_event_at_plus_1d_decile).alias("distinct_event_types_tercile"),
        F.round(F.avg("event_at_plus_1d_seq").over(w_max_event_at_plus_1d_decile_2), 2).alias("event_at_plus_1d_seq_avg_7"),
        F.round(F.avg("max_event_at_plus_1d_decile").over(w_max_event_at_plus_1d_decile_3), 2).alias("max_event_at_plus_1d_decile_avg_3_prior"),
        F.last("distinct_event_types", ignorenulls=True).over(w_max_event_at_plus_1d_decile_4).alias("last_known_distinct_event_types"),
    )
)

result = (
    with_trends
    .filter(F.col("max_event_at_plus_1d_decile_avg_3_prior") > 2.4)
    .select(
        "event_at_plus_1d_seq",
        "distinct_event_types",
        "max_event_at_plus_1d_decile",
        "total_minutes_since_prev_event_at_plus_1d",
        "distinct_event_types_tercile",
        "max_event_at_plus_1d_decile_avg_3_prior",
        "last_known_distinct_event_types",
        (F.col("event_at_plus_1d_seq") - F.col("distinct_event_types")).alias("event_at_plus_1d_seq_minus_distinct_event_types"),
        (F.col("distinct_event_types") + F.col("event_at_plus_1d_seq")).alias("distinct_event_types_plus_event_at_plus_1d_seq"),
    )
    .orderBy("distinct_event_types_tercile")
)
