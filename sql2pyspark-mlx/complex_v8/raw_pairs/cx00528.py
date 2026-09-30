from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_actor_len_by_actor_len_prior = Window.partitionBy("actor_len").orderBy("actor_len", "id").rowsBetween(-1, -1)
w_actor_len_by_actor_len_runni = Window.partitionBy("actor_len").orderBy("actor_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_len_by_actor_len_ties = Window.partitionBy("actor_len").orderBy("actor_len")
w_event_type_group = Window.partitionBy("event_type_group")
w_latest_2 = Window.partitionBy("event_type_group", "event_type").orderBy(F.col("actor_len_cume_dist").desc(), F.col("id").desc())

prepared_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-24 11:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
)

latest = (
    prepared_events
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

tagged = (
    latest
    .withColumn("event_type_group", F.when(F.col("event_type").isin("reopened", "replied"), "group_a").when(F.col("event_type") == "escalated", "group_b").otherwise("other"))
)

annotated = (
    tagged
    .select(
        "id",
        "event_at",
        "event_type",
        "event_type_group",
        F.concat_ws(" / ", F.col("event_type_group"), F.col("actor")).alias("event_type_group_actor_label"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
)

ranked = (
    annotated
    .select(
        "id",
        "event_type",
        "event_type_group",
        "actor_len",
        F.sum("actor_len").over(w_actor_len_by_actor_len_prior).alias("actor_len_sum_1_prior"),
        F.first("actor_len", ignorenulls=True).over(w_actor_len_by_actor_len_runni).alias("first_known_actor_len"),
        F.round(F.cume_dist().over(w_actor_len_by_actor_len_ties), 3).alias("actor_len_cume_dist"),
        F.max("actor_len").over(w_event_type_group).alias("event_type_group_max_actor_len"),
    )
)

latest_per_key = (
    ranked
    .withColumn("row_num", F.row_number().over(w_latest_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .select(
        "id",
        "actor_len",
        "actor_len_sum_1_prior",
        "first_known_actor_len",
        "actor_len_cume_dist",
        "event_type_group_max_actor_len",
        F.round(F.col("first_known_actor_len") / F.nullif(F.col("actor_len_cume_dist"), F.lit(0)), 3).alias("first_known_actor_len_per_actor_len_cume_dist"),
        F.abs(F.col("first_known_actor_len") - 10).alias("first_known_actor_len_distance"),
    )
)
