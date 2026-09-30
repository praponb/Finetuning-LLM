from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_event_at_year_actor_by_event = Window.partitionBy(F.year("event_at"), "actor").orderBy("event_at")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").asc_nulls_last())
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id")
w_event_at_year_actor_by_event_2 = Window.partitionBy(F.year("event_at"), "actor").orderBy("event_at", "id")
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_2 = Window.orderBy(F.col("event_at").asc_nulls_last(), "id")
w_first_known_event_at_year_ac = Window.partitionBy(F.year("first_known_event_at"), "actor").orderBy(F.col("event_at_cume_dist_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_known_event_at_year_ac_2 = Window.partitionBy(F.year("first_known_event_at"), "actor").orderBy(F.col("event_at_cume_dist_2").desc(), "id").rowsBetween(-6, Window.currentRow)
w_prev2_event_at_to_date_secon = Window.partitionBy(F.to_date("prev2_event_at"), F.date_trunc("month", "second_event_at")).orderBy("event_at_cume_dist", "id").rowsBetween(-1, -1)
w_prev2_event_at_to_date_secon_2 = Window.partitionBy(F.to_date("prev2_event_at"), F.date_trunc("month", "second_event_at")).orderBy("event_at_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_rank_second_event_a = Window.partitionBy("event_at_rank", F.year("second_event_at"))
w_n_actor_customer_prev2_event = Window.partitionBy("n_actor_customer", "prev2_event_at_2_year").orderBy(F.col("n_actor_customer").desc(), "prev2_event_at_2_year")
w_n_rows = Window.partitionBy("n_rows")

cleaned_events = df_ticket_events.filter(F.col("ticket_id").isNotNull())

windowed = (
    cleaned_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.rank().over(w_event_at_year_actor_by_event).alias("event_at_rank"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.first("event_at", ignorenulls=True).over(w_event_type_actor_by_event_at).alias("first_known_event_at"),
        F.lag("event_at", 2).over(w_actor_by_event_at).alias("prev2_event_at"),
        F.round(F.percent_rank().over(w_event_at_year_actor_by_event_2), 3).alias("event_at_percent_rank"),
        F.nth_value("event_at", 2).over(w_actor_by_event_at_full).alias("second_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_at_2), 3).alias("event_at_cume_dist_2"),
        F.lag("event_at", 2).over(w_all_by_event_at_2).alias("prev2_event_at_2"),
    )
)

leaders = windowed.filter((F.col("event_at_rank") == 1) & (F.month(F.col("event_at")).isin(1, 3)))

ranked = (
    leaders
    .select(
        "id",
        "ticket_id",
        "actor",
        "minutes_since_prev_event_at",
        "event_at_rank",
        "event_at_cume_dist",
        "first_known_event_at",
        "prev2_event_at",
        "second_event_at",
        "prev2_event_at_2",
        F.nth_value("event_at", 2).over(w_first_known_event_at_year_ac).alias("second_event_at_2"),
        F.round(F.avg("minutes_since_prev_event_at").over(w_first_known_event_at_year_ac_2), 2).alias("minutes_since_prev_event_at_avg_7"),
        F.round(F.avg("event_at_cume_dist").over(w_prev2_event_at_to_date_secon), 2).alias("event_at_cume_dist_avg_1_prior"),
        F.first("prev2_event_at").over(w_prev2_event_at_to_date_secon_2).alias("first_prev2_event_at"),
        F.count(F.lit(1)).over(w_event_at_rank_second_event_a).alias("rows_in_event_at_rank_second_event_at_year"),
    )
)

per_group = (
    ranked
    .groupBy(F.year("prev2_event_at_2").alias("prev2_event_at_2_year"))
    .agg(
        F.sum(F.when(F.col("actor") == "customer", 1).otherwise(0)).alias("n_actor_customer"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

with_windows = (
    per_group
    .select(
        "*",
        F.round(F.cume_dist().over(w_n_actor_customer_prev2_event), 3).alias("n_actor_customer_cume_dist"),
        F.max("n_actor_customer").over(w_n_rows).alias("n_rows_max_n_actor_customer"),
    )
)

result = (
    with_windows
    .select(
        "prev2_event_at_2_year",
        "n_actor_customer_cume_dist",
        "n_rows_max_n_actor_customer",
        F.round(F.col("n_actor_customer") / F.nullif(F.col("prev2_event_at_2_year"), F.lit(0)), 3).alias("n_actor_customer_per_prev2_event_at_2_year"),
        F.round(F.col("n_actor_customer") / F.nullif(F.col("prev2_event_at_2_year"), F.lit(0)), 3).alias("n_actor_customer_per_prev2_event_at_2_year_2"),
    )
    .orderBy(
        F.col("n_rows_max_n_actor_customer").desc_nulls_first(),
        F.col("prev2_event_at_2_year").asc_nulls_last(),
    )
)
