from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_id", "actor").orderBy("event_at_plus_15m", "id")

cleaned_events = (
    df_ticket_events
    .filter(F.col("event_type").isin("reopened", "replied"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
        (F.col("event_at") + F.expr("INTERVAL 15 MINUTES")).alias("event_at_plus_15m"),
    )
)

latest = (
    cleaned_events
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .filter(F.col("actor") == "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_prefix",
        "event_at_plus_15m",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.concat(F.col("actor_prefix"), F.lit("#"), F.col("id").cast("string")).alias("actor_prefix_ref"),
    )
    .orderBy(F.col("event_type_len").desc())
)
