from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").desc(), F.col("id").desc())

base_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")) & (F.col("actor").isin("agent", "system")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

latest_per_key = (
    base_events
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select("actor", "event_type")
    .distinct()
    .orderBy(F.col("event_type").desc())
)
