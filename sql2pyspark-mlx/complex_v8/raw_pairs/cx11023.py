from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc())
w_all_by_event_at_moving_6 = Window.orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_ticket_id_by_event_at_runnin = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at", "id")
w_ticket_id = Window.partitionBy("ticket_id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc_nulls_first())
w_event_type = Window.partitionBy("event_type")
w_first_event_at_year = Window.partitionBy("first_event_at_year")
w_event_at_month_by_last_known = Window.partitionBy(F.month("event_at")).orderBy("last_known_event_at", "id")
w_event_at_month_by_last_known_2 = Window.partitionBy(F.month("event_at")).orderBy("last_known_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_rows = Window.partitionBy("n_rows")

cleaned = (
    df_ticket_events
    .filter((F.col("event_type") == "replied") | (F.col("actor") == "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.year(F.col("event_at")).alias("event_at_year"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

ranked = (
    cleaned
    .select(
        "id",
        "event_at",
        "event_type",
        "event_at_year",
        F.first("event_at_year").over(w_ticket_id_by_event_at_full).alias("first_event_at_year"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.round(F.avg("event_at_year").over(w_all_by_event_at_moving_6), 2).alias("event_at_year_avg_7"),
        F.last("event_at", ignorenulls=True).over(w_ticket_id_by_event_at_runnin).alias("last_known_event_at"),
        F.dense_rank().over(w_actor_event_type_by_event_at).alias("event_at_dense_rank"),
        F.size(F.collect_set("actor").over(w_ticket_id)).alias("distinct_actor_in_ticket_id"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist_2"),
        F.round(F.col("event_at_year") / F.nullif(F.sum("event_at_year").over(w_event_type), F.lit(0)), 4).alias("event_at_year_share_of_event_type"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_percent_rank"),
    )
)

sequenced = (
    ranked
    .select(
        "id",
        "event_type",
        "event_at_year",
        "first_event_at_year",
        "event_at_cume_dist",
        "event_at_year_avg_7",
        "last_known_event_at",
        "event_at_dense_rank",
        "distinct_actor_in_ticket_id",
        "event_at_year_share_of_event_type",
        "event_at_percent_rank",
        F.round(F.col("distinct_actor_in_ticket_id") - F.avg("distinct_actor_in_ticket_id").over(w_first_event_at_year), 2).alias("distinct_actor_in_ticket_id_vs_first_event_at_ye"),
        F.lag("event_at_percent_rank").over(w_event_at_month_by_last_known).alias("prev_event_at_percent_rank"),
        F.round(F.cume_dist().over(w_event_at_month_by_last_known), 3).alias("last_known_event_at_cume_dist"),
        F.last("event_at_cume_dist").over(w_event_at_month_by_last_known_2).alias("last_event_at_cume_dist"),
        F.dense_rank().over(w_event_at_month_by_last_known).alias("last_known_event_at_dense_rank"),
        F.row_number().over(w_event_at_month_by_last_known).alias("seq_no"),
        F.lag("event_at_cume_dist_2").over(w_event_at_month_by_last_known).alias("prev_event_at_cume_dist_2"),
    )
)

stats = (
    sequenced
    .groupBy(
        "distinct_actor_in_ticket_id",
        "event_at_dense_rank",
        "last_known_event_at_dense_rank",
    )
    .agg(
        F.sum("event_at_year").alias("total_event_at_year"),
        F.count("*").alias("n_rows"),
        F.sum("last_known_event_at_cume_dist").alias("total_last_known_event_at_cume_dist"),
        F.sum(F.when(~F.col("event_type").isin("reopened", "created"), 1).otherwise(0)).alias("n_event_type_not_in_reopened_c"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
    )
)

ranked_2 = (
    stats
    .select(
        "distinct_actor_in_ticket_id",
        "event_at_dense_rank",
        "last_known_event_at_dense_rank",
        "total_event_at_year",
        "n_rows",
        "total_last_known_event_at_cume_dist",
        "n_event_type_not_in_reopened_c",
        "distinct_event_types",
        F.round(F.col("n_rows") - F.avg("n_rows").over(w_n_rows), 2).alias("n_rows_vs_n_rows_avg"),
        F.round(F.avg("event_at_dense_rank").over(w_n_rows), 2).alias("n_rows_avg_event_at_dense_rank"),
    )
)

result = (
    ranked_2
    .filter(F.col("total_last_known_event_at_cume_dist") < 3.6)
    .select(
        "distinct_actor_in_ticket_id",
        "event_at_dense_rank",
        "total_event_at_year",
        "n_rows",
        "n_event_type_not_in_reopened_c",
        "distinct_event_types",
        "n_rows_avg_event_at_dense_rank",
        (F.floor(F.col("n_rows_vs_n_rows_avg") / 5) * 5).alias("n_rows_vs_n_rows_avg_bucket"),
        (F.col("n_rows_vs_n_rows_avg") - F.col("distinct_event_types")).alias("n_rows_vs_n_rows_avg_minus_distinct_event_types"),
    )
    .orderBy(
        F.col("distinct_actor_in_ticket_id").desc(),
        F.col("n_event_type_not_in_reopened_c").desc(),
    )
)
