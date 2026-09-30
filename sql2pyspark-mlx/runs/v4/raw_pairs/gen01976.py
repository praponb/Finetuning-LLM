from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

filtered = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & F.col("browser").isin(["Chrome", "Safari"]))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        F.concat_ws(",", "browser", "device").alias("browser_device_key"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"), F.col("c.event_time"))
)
