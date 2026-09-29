from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 23) & (F.col("event_type") == "replied"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
)

tagged = (
    base
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        (F.floor(F.col("actor_len") / 2) * 2).alias("actor_len_bucket"),
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

result = (
    tagged
    .groupBy("event_at", "ticket_id")
    .pivot("actor", ["system", "agent"])
    .agg(F.max("actor_len_bucket"))
)
