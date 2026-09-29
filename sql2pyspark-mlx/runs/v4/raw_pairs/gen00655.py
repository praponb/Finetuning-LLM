from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

valid = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2024-03-14").cast("date")) & F.col("country").isNotNull())
    .withColumn("started_at_day", F.col("started_at").cast("date"))
)

result = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page").alias("web_event_page"))
)
